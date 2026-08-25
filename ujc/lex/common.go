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

type LexID struct {
	ParentID   string `json:"parentId"`
	ID         string `json:"id"`
	GroupOrder int    `json:"groupOrder"`
	Homonym    int    `json:"homonym"`
}

type LexItem struct {
	Lemma       string `json:"lemma"`
	Pos         string `json:"pos"`
	Gender      string `json:"gender"`
	Aspect      string `json:"aspect"`
	Uninflected bool   `json:"uninflected"`
	Plurality   int    `json:"plurality"`

	Sources map[Source][]LexID `json:"sources"`
}

func (li *LexItem) Equals(item LexItem) bool {
	return (li.Lemma == item.Lemma &&
		li.Pos == item.Pos &&
		li.Gender == item.Gender &&
		li.Aspect == item.Aspect &&
		li.Uninflected == item.Uninflected &&
		li.Plurality == item.Plurality)
}

func (li *LexItem) HasSource(source Source) bool {
	_, ok := li.Sources[source]
	return ok
}
