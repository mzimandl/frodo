// Copyright 2026 Martin Zimandl <martin.zimandl@gmail.com>
// Copyright 2026 Institute of the Czech National Corpus,
// Faculty of Arts, Charles University
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lex

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/czcorpus/cnc-gokit/collections"
	"github.com/czcorpus/cnc-gokit/util"
	"github.com/rs/zerolog/log"
)

type Source string

const (
	SourceASSC Source = "assc"
	SourceIJP  Source = "ijp"
	SourceSSJC Source = "ssjc"
	SourcePSJC Source = "psjc"
	SourceSJC  Source = "sjc"
	SourceSSC  Source = "ssc"

	POSAdj   = "A"
	POSAbb   = "B"
	PosNum   = "C"
	POSAdv   = "D"
	POSFore  = "F"
	POSInter = "I"
	POSConj  = "J"
	POSNoun  = "N"
	POSPron  = "P"
	POSPrep  = "R"
	POSSegm  = "S"
	POSPart  = "T"
	POSVerb  = "V"
	POSUnkn  = "X"
	POSPunc  = "Z"
	POSDTIJ  = "DTIJ"

	GenderMascAnim     = "M"
	GenderMascInan     = "I"
	GenderMascAnimInan = "B"
	GenderFem          = "F"
	GenderNeut         = "N"

	AspectPerf = "P"
	AspectImp  = "I"
	AspectBoth = "B"

	UninflectedFalse = 0
	UninflectedTrue  = 1

	PluralityNone    = 0
	PluralityPlural  = 1
	PluralityAlways  = 2
	PluralityUsually = 3
	PluralityOnly    = 4
	PluralityUnknown = 5

	TableName = "lex_dictionary"
)

var dictionaryTable = `
CREATE TABLE %s (
	group_id VARCHAR(100),
	homonym TINYINT DEFAULT 0 NOT NULL,
	group_order TINYINT DEFAULT 0 NOT NULL,

	lemma VARCHAR(100) NOT NULL,
	pos VARCHAR(4) NOT NULL,
	gender VARCHAR(1),
	aspect VARCHAR(1),
	uninflected TINYINT DEFAULT 0 NOT NULL,
	plurality TINYINT DEFAULT 0 NOT NULL,
	
	source VARCHAR(8) NOT NULL,
	external_id VARCHAR(100) NOT NULL,
	external_parent_id VARCHAR(100),

	-- This column automatically calculates the normalized search key
	search_key VARCHAR(100) COLLATE utf8mb4_unicode_ci GENERATED ALWAYS AS (
		REPLACE(REPLACE(LOWER(lemma), 'y', 'i'), 'z', 's')
	) STORED
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`

func CreateTables(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", TableName)); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(dictionaryTable, TableName)); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}
	return tx, nil
}

func SearchTypoSuggestions(ctx context.Context, db *sql.DB, term string) ([]string, error) {
	row, err := db.QueryContext(
		ctx,
		"SELECT DISTINCT lemma "+
			"FROM lex_dictionary "+
			"WHERE search_key = REPLACE(REPLACE(LOWER(?), 'y', 'i'), 'z', 's');",
		term,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to search match: %w", err)
	}
	defer row.Close()

	suggestions := make([]string, 0, 5)
	for row.Next() {
		var lemma string
		if err := row.Scan(&lemma); err != nil {
			if err == sql.ErrNoRows {
				return suggestions, nil
			}
			return nil, fmt.Errorf("failed to scan suggestions: %w", err)
		}
		suggestions = append(suggestions, lemma)
	}
	suggestions = collections.SliceFilter(suggestions, func(v string, i int) bool {
		return !strings.EqualFold(v, term)
	})
	return suggestions, nil
}

func SearchAvailableSources(ctx context.Context, db *sql.DB, lemma string) ([]Source, error) {
	row, err := db.QueryContext(
		ctx,
		"SELECT DISTINCT source "+
			"FROM lex_dictionary "+
			"WHERE lemma = ?;",
		lemma,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to search available sources: %w", err)
	}
	defer row.Close()

	sources := make([]Source, 0)
	for row.Next() {
		var source Source
		if err := row.Scan(&source); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, fmt.Errorf("failed to scan available source: %w", err)
		}
		sources = append(sources, source)
	}

	return sources, nil
}

func SearchVariants(ctx context.Context, db *sql.DB, lemma string, mainSource Source) ([]LexItem, error) {
	row, err := db.QueryContext(
		ctx,
		`
			-- find available variants, get exact lemmata and their variants based on group_id and source
			SELECT DISTINCT lemma, pos, gender, aspect, uninflected, plurality
			FROM lex_dictionary AS l
			JOIN (
				SELECT DISTINCT group_id, source
				FROM lex_dictionary
				WHERE lemma = ? AND source = ? AND group_id IS NOT NULL
			) AS g
			ON g.group_id = l.group_id AND g.source = l.source
			UNION
			SELECT DISTINCT lemma, pos, gender, aspect, uninflected, plurality
			FROM lex_dictionary AS l
			WHERE lemma = ? AND source = ? AND group_id IS NULL
		`,
		lemma, mainSource, lemma, mainSource,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to search variants: %w", err)
	}
	defer row.Close()

	data := make([]LexItem, 0, 5)
	for row.Next() {
		var genderArg, aspectArg sql.NullString
		var uninflectedArg int64
		item := LexItem{}
		if err := row.Scan(&item.Lemma, &item.Pos, &genderArg, &aspectArg, &uninflectedArg, &item.Plurality); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, fmt.Errorf("failed to scan variants: %w", err)
		}
		item.Uninflected = uninflectedArg != 0
		if genderArg.Valid {
			item.Gender = genderArg.String
		}
		if aspectArg.Valid {
			item.Aspect = aspectArg.String
		}
		data = append(data, item)
	}

	return data, nil
}

func SearchSources(ctx context.Context, db *sql.DB, lexItem LexItem) (map[Source][]LexID, error) {
	// if lexItem.Pos is 'X', do not filter by pos (accept any pos)
	whereParts := []string{"lemma = ?"}
	args := []any{lexItem.Lemma}
	if lexItem.Pos != POSUnkn {
		whereParts = append(whereParts, "(pos = ? OR pos = 'X')")
		args = append(args, lexItem.Pos)
	}
	if lexItem.Gender != "" {
		whereParts = append(whereParts, "gender = ?")
		args = append(args, lexItem.Gender)
	} else {
		whereParts = append(whereParts, "gender is NULL")
	}
	if lexItem.Aspect != "" {
		whereParts = append(whereParts, "aspect = ?")
		args = append(args, lexItem.Aspect)
	} else {
		whereParts = append(whereParts, "aspect is NULL")
	}
	if lexItem.Plurality != PluralityUnknown {
		whereParts = append(whereParts, "(plurality = ? OR plurality = ?)")
		args = append(args, lexItem.Plurality, PluralityUnknown)
	}
	whereParts = append(whereParts, "uninflected = ?")
	args = append(args, util.Ternary(lexItem.Uninflected, 1, 0))

	query := `
		SELECT source, JSON_ARRAYAGG(JSON_OBJECT('id', external_id, 'parentId', external_parent_id, 'groupOrder', group_order, 'homonym', homonym) ORDER BY homonym) AS idents
		FROM lex_dictionary
		WHERE ` + strings.Join(whereParts, " AND ") + `
		GROUP BY source
		`

	log.Debug().Any("query", query).Any("args", args).Send()
	row, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to search sources: %w", err)
	}
	defer row.Close()

	sources := make(map[Source][]LexID)
	for row.Next() {
		var source Source
		var jsonIdents string
		if err := row.Scan(&source, &jsonIdents); err != nil {
			if err == sql.ErrNoRows {
				return sources, nil
			}
			return nil, fmt.Errorf("failed to scan sources: %w", err)
		}
		var idents []LexID
		if err := json.Unmarshal([]byte(jsonIdents), &idents); err != nil {
			return nil, fmt.Errorf("failed to unmarshal sources: %w", err)
		}
		sources[source] = idents
	}
	return sources, nil
}

func PruneData(ctx context.Context, tx *sql.Tx, source Source) error {
	_, err := tx.ExecContext(
		ctx,
		"DELETE FROM lex_dictionary WHERE source = ?",
		source,
	)
	if err != nil {
		return err
	}
	return nil
}

func SearchLexItemID(ctx context.Context, db *sql.DB, lexItem LexItem, source Source) ([]LexID, error) {
	// Build WHERE clause dynamically so empty Gender/Aspect are searched as NULL
	where := make([]string, 0, 8)
	args := make([]interface{}, 0, 8)
	where = append(where, "lemma = ?")
	args = append(args, lexItem.Lemma)
	where = append(where, "pos = ?")
	args = append(args, lexItem.Pos)

	if lexItem.Gender == "" {
		where = append(where, "gender IS NULL")
	} else {
		where = append(where, "gender = ?")
		args = append(args, lexItem.Gender)
	}

	if lexItem.Aspect == "" {
		where = append(where, "aspect IS NULL")
	} else {
		where = append(where, "aspect = ?")
		args = append(args, lexItem.Aspect)
	}

	// uninflected stored as tinyint; convert bool to int
	uninflectedInt := 0
	if lexItem.Uninflected {
		uninflectedInt = 1
	}
	where = append(where, "uninflected = ?")
	args = append(args, uninflectedInt)

	if lexItem.Plurality != PluralityUnknown {
		where = append(where, "(plurality = ? OR plurality = ?)")
		args = append(args, lexItem.Plurality, PluralityUnknown)
	}

	where = append(where, "source = ?")
	args = append(args, source)

	query := "SELECT external_id, external_parent_id, group_order, homonym FROM lex_dictionary WHERE " + strings.Join(where, " AND ")

	row, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to search the term: %w", err)
	}
	defer row.Close()

	lexIds := make([]LexID, 0, 1)
	for row.Next() {
		var lexId LexID
		var externalParentID sql.NullString
		if err := row.Scan(&lexId.ID, &externalParentID, &lexId.GroupOrder, &lexId.Homonym); err != nil {
			if err == sql.ErrNoRows {
				return lexIds, nil
			}
			return nil, fmt.Errorf("failed to scan the lex id: %w", err)
		}
		lexId.ParentID = externalParentID.String
		lexIds = append(lexIds, lexId)
	}

	return lexIds, nil
}
