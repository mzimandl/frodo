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
	"fmt"

	"github.com/czcorpus/cnc-gokit/collections"
)

type LexTransform func(context.Context, *sql.DB, []LexItem) ([]LexItem, error)

func ApplyTransformations(ctx context.Context, db *sql.DB, data []LexItem, transforms ...LexTransform) ([]LexItem, error) {
	var err error
	for _, transform := range transforms {
		if transform == nil {
			continue
		}
		data, err = transform(ctx, db, data)
		if err != nil {
			return nil, fmt.Errorf("failed to transform data: %w", err)
		}

	}
	return data, nil
}

func Identity(ctx context.Context, db *sql.DB, data []LexItem) ([]LexItem, error) {
	return data, nil
}

func TransformToDTIJ(ctx context.Context, db *sql.DB, data []LexItem) ([]LexItem, error) {
	var result []LexItem
	for _, item := range data {
		if item.Key.Pos == PosAdv || item.Key.Pos == PosPart || item.Key.Pos == PosInter || item.Key.Pos == PosConj {
			item.Key.Pos = PosDTIJ
			item.PosSource = ""
		}
		if collections.SliceFindIndex(result, func(v LexItem) bool { return item.Key == v.Key }) == -1 {
			result = append(result, item)
		}
	}

	return result, nil
}

func JoinToIBGenderFromSSC(ctx context.Context, db *sql.DB, data []LexItem) ([]LexItem, error) {
	// if data gender == I || B and no SSC source
	// add to data SSC source with gender M
	// (SSC source does not distinct masculine genders)
	for i, item := range data {
		if !item.HasSource(SourceSSC) && (item.Key.Gender == GenderMascInan || item.Key.Gender == GenderMascAnimInan) {
			search := LexKey{
				Lemma:       item.Key.Lemma,
				Pos:         item.Key.Pos,
				Gender:      GenderMascAnim,
				Aspect:      item.Key.Aspect,
				Uninflected: item.Key.Uninflected,
				Plurality:   0,
			}
			ids, err := SearchLexItemID(ctx, db, search, SourceSSC)
			if err != nil {
				return nil, fmt.Errorf("failed to join masculine gender SSC data: %w", err)
			}
			if len(ids) != 0 {
				data[i].Sources[SourceSSC] = ids
			}
		}
	}
	return data, nil
}
