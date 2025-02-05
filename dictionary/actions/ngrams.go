// Copyright 2022 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2022 Institute of the Czech National Corpus,
//                Faculty of Arts, Charles University
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

package actions

import (
	"encoding/json"
	"errors"
	"fmt"
	"frodo/common"
	"frodo/db/mysql"
	"frodo/liveattrs/db/freqdb"
	"frodo/liveattrs/laconf"
	"io"
	"net/http"
	"path/filepath"

	"github.com/gin-gonic/gin"

	"github.com/czcorpus/cnc-gokit/strutil"
	"github.com/czcorpus/cnc-gokit/unireq"
	"github.com/czcorpus/cnc-gokit/uniresp"
)

type reqArgs struct {
	ColMapping *freqdb.QSAttributes   `json:"colMapping,omitempty"`
	PosTagset  common.SupportedTagset `json:"posTagset"`
}

func (args reqArgs) Validate() error {
	if err := args.PosTagset.Validate(); err != nil {
		return fmt.Errorf("failed to validate tagset: %w", err)
	}

	if args.ColMapping != nil {
		tmp := make(map[int]int)
		tmp[args.ColMapping.Lemma]++
		tmp[args.ColMapping.Sublemma]++
		tmp[args.ColMapping.Word]++
		tmp[args.ColMapping.Tag]++

		if len(tmp) < 4 {
			return errors.New(
				"each of the lemma, sublemma, word, tag must be mapped to a unique table column")
		}
	}
	return nil
}

func (a *Actions) getNgramArgs(req *http.Request) (reqArgs, error) {
	var jsonArgs reqArgs
	err := json.NewDecoder(req.Body).Decode(&jsonArgs)
	if err == io.EOF {
		err = nil
	}
	return jsonArgs, err
}

func (a *Actions) GenerateNgrams(ctx *gin.Context) {
	corpusID := ctx.Param("corpusId")
	appendMode := ctx.Request.URL.Query().Get("append") == "1"
	ngramSize, ok := unireq.GetURLIntArgOrFail(ctx, "ngramSize", 1)
	if !ok {
		return
	}

	args, err := a.getNgramArgs(ctx.Request)
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusBadRequest)
		return
	}
	if err = args.Validate(); err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusUnprocessableEntity)
		return
	}

	var tagset common.SupportedTagset

	if args.ColMapping == nil {
		regPath := filepath.Join(a.corpConf.RegistryDirPaths[0], corpusID) // TODO the [0]

		var corpTagsets []common.SupportedTagset
		var err error

		if args.PosTagset != "" {
			corpTagsets = []common.SupportedTagset{args.PosTagset}

		} else {
			corpTagsets, err = a.cncDB.GetCorpusTagsets(corpusID)
			if err != nil {
				uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
				return
			}
		}
		tagset = common.GetFirstSupportedTagset(corpTagsets)
		if tagset == "" {
			avail := strutil.JoinAny(corpTagsets, func(v common.SupportedTagset) string { return v.String() }, ", ")
			uniresp.RespondWithErrorJSON(
				ctx, fmt.Errorf(
					"cannot find a suitable default tagset for %s (found: %s)",
					corpusID, avail,
				),
				http.StatusUnprocessableEntity,
			)
			return
		}
		attrMapping, err := common.InferQSAttrMapping(regPath, tagset)
		if err != nil {
			uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
			return
		}
		args.ColMapping = &attrMapping
		// now we need to revalidate to make sure the inference provided correct setup
		if err = args.Validate(); err != nil {
			uniresp.RespondWithErrorJSON(ctx, err, http.StatusUnprocessableEntity)
			return
		}

	} else {
		tagset = args.PosTagset
	}

	laConf, err := a.laConfCache.Get(corpusID)
	if err == laconf.ErrorNoSuchConfig {
		uniresp.RespondWithErrorJSON(
			ctx,
			err,
			http.StatusNotFound,
		)
		return

	} else if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}
	// the args.ColMapping.Tag arg below is likely OK,
	// but in such case, do we need args.ColMapping.Tag?
	// TODO !!! we probably do not need the ApplyPosProperties at all,
	// because the transformation is performed earlier in the liveattrs part
	// ([corpus]_colcounts table)
	posFn, err := common.ApplyPosProperties(&laConf.Ngrams, args.ColMapping.Tag, tagset)
	if err == common.ErrorPosNotDefined {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusUnprocessableEntity)
		return

	} else if err != nil {
		uniresp.RespondWithErrorJSON(
			ctx,
			err,
			http.StatusInternalServerError,
		)
		return
	}

	corpusDBInfo, err := a.cncDB.LoadInfo(corpusID)
	if err != nil {
		uniresp.RespondWithErrorJSON(
			ctx,
			err,
			http.StatusInternalServerError,
		)
		return
	}

	tunedDb, err := mysql.OpenImportTunedDB(a.laDB.Conf())
	if err != nil {
		uniresp.RespondWithErrorJSON(
			ctx,
			err,
			http.StatusInternalServerError,
		)
		return
	}
	generator := freqdb.NewNgramFreqGenerator(
		tunedDb,
		a.jobActions,
		corpusDBInfo.GroupedName(),
		corpusDBInfo.Name,
		appendMode,
		ngramSize,
		posFn,
		*args.ColMapping,
	)
	jobInfo, err := generator.GenerateAfter(ctx.Request.URL.Query().Get("parentJobId"))
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}
	uniresp.WriteJSONResponse(ctx.Writer, jobInfo.FullInfo())
}
