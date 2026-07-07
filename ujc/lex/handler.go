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
	"cmp"
	"context"
	"fmt"
	"frodo/db/mysql"
	"frodo/dictionary"
	dictActions "frodo/dictionary/actions"
	"net/http"
	"sort"

	"github.com/agnivade/levenshtein"
	"github.com/czcorpus/cnc-gokit/collections"
	"github.com/czcorpus/cnc-gokit/uniresp"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

type LexExtraData struct {
	CorpusId   string  `json:"corpusId"`
	MainSource Source  `json:"mainSource"`
	Variant    LexItem `json:"variant"`
}

type Handler struct {
	db             *mysql.Adapter
	dictActions    *dictActions.Actions
	sourcePriority []Source
}

func (actions *Handler) getQueryMatches(ctx context.Context, corpusId, term string) ([]dictionary.Lemma, error) {
	datasetSize, err := actions.dictActions.GetDatasetSize(corpusId)
	if err != nil {
		return nil, err
	}

	ans, err := dictionary.Search(
		ctx,
		actions.db,
		corpusId,
		dictionary.SearchWithAnyValue(term),
		dictionary.SearchWithDatasetSizeForIPM(int(datasetSize)),
	)
	if err != nil {
		return []dictionary.Lemma{}, fmt.Errorf("failed to find lemma: %w", err)
	}
	if len(ans) > 0 {
		return ans, nil
	}
	return []dictionary.Lemma{}, nil
}

func (actions *Handler) searchCorpusLemma(ctx context.Context, corpusId, lemma, pos string) (*dictionary.Lemma, error) {
	if lemma == "" {
		return nil, nil
	}

	posArg := dictionary.SearchWithNoOp()
	if pos != "" {
		posArg = dictionary.SearchWithPoS(pos)
	}

	datasetSize, err := actions.dictActions.GetDatasetSize(corpusId)
	if err != nil {
		return nil, err
	}

	ans, err := dictionary.Search(
		ctx,
		actions.db,
		corpusId,
		dictionary.SearchWithLemma(lemma),
		dictionary.SearchWithDatasetSizeForIPM(int(datasetSize)),
		posArg,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to find lemma: %w", err)
	}
	if len(ans) > 0 {
		if len(ans) > 1 {
			log.Warn().Str("lemma", lemma).Str("pos", pos).Int("numMatches", len(ans)).Msg("Multiple matches found for lemma in corpus")
		}
		return &ans[0], nil
	}
	return nil, nil
}

func (actions *Handler) SearchWord(ctx *gin.Context) {
	corpusId := ctx.Param("corpusId")
	term := ctx.Param("term")

	// search corpus for possible lemmata of the word, corpus is used for lematization and to get the dataset size for IPM calculation
	bestMatches, err := actions.getQueryMatches(ctx, corpusId, term)
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}

	// if empty corpus matches, use ujc sources directly
	if len(bestMatches) == 0 {
		for _, source := range actions.sourcePriority {
			bestMatches, err = SearchMatches(ctx, actions.db.DB(), term, source)
			if err != nil {
				uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
				return
			}
			if len(bestMatches) > 0 {
				break
			}
		}
	}

	typoSuggestions, err := SearchTypoSuggestions(ctx, actions.db.DB(), term)
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}

	// no matches found in any source, return empty result
	if len(bestMatches) == 0 {
		ans := map[string]any{
			"matches":     make([]dictionary.Lemma, 0),
			"suggestions": typoSuggestions,
		}
		uniresp.WriteJSONResponse(ctx.Writer, ans)
		return
	}

	// remove lemma duplicates
	bestMatches = collections.SliceReduce(bestMatches, func(acc []dictionary.Lemma, curr dictionary.Lemma, i int) []dictionary.Lemma {
		if collections.SliceFindIndex(acc, func(item dictionary.Lemma) bool {
			return item.Lemma == curr.Lemma
		}) == -1 {
			// just bare minimum for WaG to process the match
			acc = append(acc, dictionary.Lemma{
				ID:        curr.ID,
				Lemma:     curr.Lemma,
				PoS:       curr.PoS,
				Forms:     []dictionary.Form{{Value: curr.Lemma, Sublemma: curr.Lemma}},
				Sublemmas: []dictionary.Sublemma{{Value: curr.Lemma}},
			})
		}
		return acc
	}, make([]dictionary.Lemma, 0, len(bestMatches)))

	// sort matches by their similarity to the query term using Levenshtein distance
	sort.Slice(bestMatches, func(i, j int) bool {
		return levenshtein.ComputeDistance(term, bestMatches[i].Lemma) < levenshtein.ComputeDistance(term, bestMatches[j].Lemma)
	})

	// we get data for first match, the rest are suggestions
	bestMatch := bestMatches[0]
	suggestions := collections.SliceMap(bestMatches[1:], func(match dictionary.Lemma, i int) string { return match.Lemma })

	lexItems, err := SearchVariants(ctx, actions.db.DB(), bestMatch.Lemma)
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}
	if lexItems == nil {
		ans := map[string]any{
			"matches":     []dictionary.Lemma{bestMatch},
			"suggestions": suggestions,
		}
		uniresp.WriteJSONResponse(ctx.Writer, ans)
		return
	}

	var mainSource Source
	// filter data by source priority
	for _, source := range actions.sourcePriority {
		filtered := collections.SliceFilter(lexItems, func(lexItem LexItem, i int) bool {
			_, ok := lexItem.Sources[source]
			return ok
		})
		if len(filtered) > 0 {
			lexItems = filtered
			mainSource = source
			break
		}
	}

	variants := make([]dictionary.Lemma, 0, len(lexItems))
	for i, item := range lexItems {
		corpusEntry, err := actions.searchCorpusLemma(ctx, corpusId, item.Lemma, item.Pos)
		if err != nil {
			uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
			return
		}
		if corpusEntry == nil {
			corpusEntry = &dictionary.Lemma{
				ID:        fmt.Sprintf("lex-%d", i),
				Lemma:     item.Lemma,
				PoS:       item.Pos,
				Specifier: cmp.Or(item.Gender, item.Aspect),
				Forms:     []dictionary.Form{{Value: item.Lemma, Sublemma: item.Lemma}},
				Sublemmas: []dictionary.Sublemma{{Value: item.Lemma}},
			}
		} else {
			corpusEntry.Specifier = cmp.Or(corpusEntry.Specifier, cmp.Or(item.Gender, item.Aspect))
			corpusEntry.Sublemmas = collections.SliceFilter(corpusEntry.Sublemmas, func(sublemma dictionary.Sublemma, i int) bool {
				return sublemma.Value == item.Lemma
			})
			corpusEntry.Forms = collections.SliceFilter(corpusEntry.Forms, func(form dictionary.Form, i int) bool {
				return form.Sublemma == item.Lemma
			})
		}
		corpusEntry.ExtraData = LexExtraData{
			CorpusId:   corpusId,
			MainSource: mainSource,
			Variant:    item,
		}
		variants = append(variants, *corpusEntry)
	}

	ans := map[string]any{
		"matches":     variants,
		"suggestions": append(suggestions, typoSuggestions...),
	}
	uniresp.WriteJSONResponse(ctx.Writer, ans)
}

func NewHandler(db *mysql.Adapter, dictActions *dictActions.Actions) *Handler {
	return &Handler{
		db:             db,
		dictActions:    dictActions,
		sourcePriority: []Source{SourceASSC, SourceIJP},
	}
}
