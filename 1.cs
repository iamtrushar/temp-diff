//--------------------------------------------------
        /// <summary>
        /// Roll up the mapping level polygons to the higher levels,
        /// reset map status code.
        /// </summary>
        /// <remarks>
        /// Dissection is not supported at the moment.
        /// </remarks>
        private bool MassAgreementRollup(Guid agreementId)
        {
            var success = true;

            var agreementLevel = this._dalService.GetAgreement(agreementId);
            if (agreementLevel is null)
            {
                return true;
            }

            Geometry geometryBagTract = null;
            Geometry geometryBagAgreement = null;
            Geometry geometryCollectionAgmtWithOri = null;
            Geometry geometryCollectionAgmtWithoutOri = null;

            using var geodatabaseTract = this._lpmSpatial.TractLayerConnection.OpenEnterpriseGeodatabase();
            using var tractFeatureClass = geodatabaseTract.OpenFeatureClass(this._lpmSpatial.TractLayerConnection);

            using var geodatabaseLegal = this._lpmSpatial.LegalLayerConnection.OpenEnterpriseGeodatabase();
            using var legalFeatureClass = geodatabaseLegal.OpenFeatureClass(this._lpmSpatial.LegalLayerConnection);


            var codes = this
                ._dalService
                .GetMapStatuses()
                .GetCanRollupMapStatusCodes();

            // for mapping level tract
            var mappingLevel = agreementLevel.MAPPING_LEVEL;
            var agreementSpatialId = agreementLevel.SPATIAL_ID;
            // ReSharper disable once ConvertIfStatementToSwitchStatement
            if (mappingLevel == "TRACT")
            {
                var agreementPolygon = this.RollupAgreementSpatial(agreementId);

                if (!string.IsNullOrEmpty(agreementSpatialId) && agreementPolygon is null)
                {
                    // if nothing rolls up i.e. agreement polygon is null
                    // then delete the agreement spatial
                    this.DeleteAgreementSpatial(agreementSpatialId, setLowerMapStatus: false);
                }
            }

            var agmtActiveIndicator = agreementLevel.ACTIVE_INDICATOR;
            // for mapping level tract
            if (mappingLevel == "LEGAL")
            {
                var legals = this
                    ._dalService
                    .GetLegalsForAgreement(agreementId)
                    .Where(r => !string.IsNullOrEmpty(r.SPATIAL_ID) && codes.Contains(r.MAP_STATUS_CODE))
                    .AsReadOnlyCollection();

                // get legals with tract order by TractId
                var legalsWithTract = legals
                    .Where(r => r.TRACT_ID is not null)
                    .OrderBy(r => r.TRACT_ID)
                    .AsReadOnlyCollection();

                // mutable, the tract polygon will grow by appending legals
                Geometry tractRollupPoly = null;
                var lastTractId = Guid.Empty;

                // process legals per tract
                bool InsertTractRollupPolygon(Guid tractId)
                {
                    if (tractRollupPoly is null)
                    {
                        return false;
                    }

                    var tractDetail = this
                        ._dalService
                        .GetTractDetail(agreementId, tractId);
                    if (tractDetail is not null
                        && this._lpmTractSpatial.InsertTractSpatial(
                            tractDetail.TRACT_DETAIL_ID,
                            tractRollupPoly,
                            mapStatusCode: this._lpmTractSpatial.CalcMapStatus(tractDetail.TRACT_DETAIL_ID),
                            setLowerMapStatus: false,
                            overwriteExisting: !string.IsNullOrEmpty(tractDetail.SPATIAL_ID)))
                    {
                        // union with tract bag
                        geometryBagTract = geometryBagTract is null
                            ? tractRollupPoly
                            : GeometryEngine.Instance.Union(tractRollupPoly, geometryBagTract);

                        var lastTractActiveIndicator = tractDetail.ACTIVE_INDICATOR;
                        if (agmtActiveIndicator == lastTractActiveIndicator)
                        {
                            var tractView = this
                                ._dalService
                                .GetGisTractViewWithTractDetailId(tractDetail.TRACT_DETAIL_ID)
                                .RequireNotNullValue();

                            var spatialId = tractView.SPATIAL_ID;
                            if (!string.IsNullOrEmpty(spatialId) && tractFeatureClass.DoesFeatureExist(spatialId))
                            {
                                var feature = tractFeatureClass.GetFeature(spatialId);
                                if (tractView.CO_ORI == 0)
                                {
                                    geometryCollectionAgmtWithOri = geometryCollectionAgmtWithOri is null
                                        ? feature!.GetShape()
                                        : GeometryEngine.Instance.Union(feature!.GetShape(), geometryCollectionAgmtWithOri);
                                }
                                else
                                {
                                    geometryCollectionAgmtWithoutOri = geometryCollectionAgmtWithoutOri is null
                                        ? feature!.GetShape()
                                        : GeometryEngine.Instance.Union(feature!.GetShape(), geometryCollectionAgmtWithoutOri);
                                }
                            }
                        }

                        // once tract is inserted, we can null the polygon and start collecting again
                        tractRollupPoly = null;

                        return true;
                    }
                    return false;
                }

                var tractPolygonInsertCounter = 0;
                foreach (var legal in legalsWithTract.Where(l => l.TRACT_ID.HasValue))
                {
                    Geometry lastLegal = null;

                    var legalSpatialId = legal.SPATIAL_ID;
                    if (!string.IsNullOrEmpty(legalSpatialId) && legalFeatureClass.DoesFeatureExist(legalSpatialId))
                    {
                        lastLegal = legalFeatureClass.GetFeature(legalSpatialId).GetShape();
                    }

                    var currentTractId = (Guid)legal.TRACT_ID;
                    if (lastTractId == Guid.Empty || currentTractId == lastTractId)
                    {
                        lastTractId = currentTractId;

                        // keep union-ing legals
                        if (lastLegal is not null)
                        {
                            tractRollupPoly = tractRollupPoly is null
                                ? lastLegal
                                : GeometryEngine.Instance.Union(lastLegal, tractRollupPoly);
                        }
                    }
                    else if (currentTractId != lastTractId)
                    {
                        var lastTractDetail = this._dalService.GetTractDetail(agreementId, lastTractId);
                        if (lastTractDetail is not null)
                        {
                            var lastTractSpatialId = lastTractDetail.SPATIAL_ID;
                            if (!string.IsNullOrEmpty(lastTractSpatialId)
                                && tractFeatureClass.DoesFeatureExist(lastTractSpatialId)
                                // has any MSC 14 legals i.e. mapped at tract mapping level, if so append the tract
                                && this.GetLegalsMappedAtHigherLevel(agreementId, lastTractId).Any())
                            {
                                // get the existing tract polygon
                                var tractGeometry = tractFeatureClass.GetFeature(lastTractSpatialId).GetShape();
                                tractRollupPoly = tractRollupPoly is null
                                    ? tractGeometry
                                    : GeometryEngine.Instance.Union(tractGeometry, tractRollupPoly);
                            }

                            if (InsertTractRollupPolygon(lastTractId))
                            {
                                tractPolygonInsertCounter++;
                            }

                            if (lastLegal is not null)
                            {
                                tractRollupPoly = lastLegal;
                            }
                        }

                        lastTractId = currentTractId;
                    }
                }

                // last tract to inserted after exiting the loop
                if (lastTractId != Guid.Empty && InsertTractRollupPolygon(lastTractId))
                {
                    tractPolygonInsertCounter++;
                }

                // get legals without tract
                var legalsWithoutTracts = legals.Where(r => r.TRACT_ID is null);

                // ReSharper disable once LoopCanBeConvertedToQuery
                foreach (var row in legalsWithoutTracts)
                {
                    var legalSpatialId = row.SPATIAL_ID;
                    if (!string.IsNullOrEmpty(legalSpatialId) && legalFeatureClass.DoesFeatureExist(legalSpatialId))
                    {
                        var feature = legalFeatureClass.GetFeature(legalSpatialId).GetShape();
                        geometryBagAgreement = geometryBagAgreement is null
                            ? feature
                            : GeometryEngine.Instance.Union(feature, geometryBagAgreement);
                    }
                }


                // if nothing got mapped, then start deleting and reset map status code
                if (geometryCollectionAgmtWithOri is null
                    && geometryCollectionAgmtWithoutOri is null
                    && !string.IsNullOrEmpty(agreementSpatialId))
                {
                    // delete Agreement Polygon
                    this.DeleteAgreementSpatial(agreementSpatialId, setLowerMapStatus: false);

                    if (tractPolygonInsertCounter == 0)
                    {
                        // delete tract polygons
                        foreach (var tractDetail in this
                            ._dalService
                            .GetTractDetailsForAgreement(agreementId)
                            .Where(r => !string.IsNullOrEmpty(r.SPATIAL_ID)))
                        {
                            if (!string.IsNullOrEmpty(tractDetail.SPATIAL_ID)
                                && tractFeatureClass.DoesFeatureExist(tractDetail.SPATIAL_ID))
                            {
                                this._lpmTractSpatial.DeleteTractSpatial(
                                    tractDetail.SPATIAL_ID,
                                    setLowerMapStatus: false,
                                    cascadeDelete: false,
                                    rollup: false);
                            }
                        }
                    }

                    // handle tracts MSC = 14
                    // get the tractDetails that have MSC = Mapped at Higher Level
                    foreach (var tractDetail in this.GetTractsMappedAtHigherLevel(agreementId))
                    {
                        // set these tracts to Not Mapped
                        success = this._dalService.UpdateAgreementSpatialAttributes(
                            tractDetail.TRACT_DETAIL_ID,
                            MapStatusCodes.NotMapped,
                            spatialId: null);
                    }


                    // handle legals with MSC = 14
                    // get the legals without tracts and have MSC = Mapped at Higher Level
                    foreach (var legal in this
                        .GetLegalsMappedAtHigherLevel(agreementId)
                        .Where(r => r.TRACT_ID is null)
                        .Where(r => string.IsNullOrEmpty(r.SPATIAL_ID)))
                    {
                        // set these legal to Not Mapped
                        success = this._dalService.UpdateLegalSpatialAttributes(
                            legal.LEGAL_DESC_ID,
                            MapStatusCodes.NotMapped,
                            spatialId: null);
                    }
                }
                else if (geometryCollectionAgmtWithOri is not null
                    || geometryCollectionAgmtWithoutOri is not null)
                {
                    // insert into agreement
                    using var geodatabase = this._lpmSpatial.AgreementLayerConnection.OpenEnterpriseGeodatabase();
                    using var agreementFeatureClass = geodatabase.OpenFeatureClass(this._lpmSpatial.AgreementLayerConnection);

                    if (!string.IsNullOrEmpty(agreementSpatialId)
                        && agreementFeatureClass.DoesFeatureExist(agreementSpatialId)
                        // has mapped tract at agreement level i.e. is there any tract msc with 14
                        // then append that piece to the agreement polygon
                        && this.GetTractsMappedAtHigherLevel(agreementId).Any())
                    {
                        var agreementGeometry = agreementFeatureClass.GetFeature(agreementSpatialId).GetShape();
                        // test the first record if company ownership interest has value > 0
                        var companyOri = this
                            ._dalService
                            .GetGisTractViewWithAgreementId(agreementId)
                            .First()
                            .CO_ORI;
                        if (companyOri is > 0)
                        {
                            geometryCollectionAgmtWithOri = geometryCollectionAgmtWithOri is null
                                ? agreementGeometry
                                : GeometryEngine.Instance.Union(agreementGeometry, geometryCollectionAgmtWithOri);
                        }
                        else
                        {
                            geometryCollectionAgmtWithoutOri = geometryCollectionAgmtWithoutOri is null
                                ? agreementGeometry
                                : GeometryEngine.Instance.Union(agreementGeometry, geometryCollectionAgmtWithoutOri);
                        }
                    }

                    if (geometryCollectionAgmtWithOri is not null)
                    {
                        // trusharm - add a defensive check if the agreement view has ori == 0
                        geometryBagAgreement = geometryBagAgreement is null
                            ? geometryCollectionAgmtWithOri
                            : GeometryEngine.Instance.Union(geometryCollectionAgmtWithOri, geometryBagAgreement);
                    }
                    else if (geometryCollectionAgmtWithoutOri is not null)
                    {
                        // trusharm - add a defensive check if the agreement view has ori > 0
                        geometryBagAgreement = geometryBagAgreement is null
                            ? geometryCollectionAgmtWithoutOri
                            : GeometryEngine.Instance.Union(geometryCollectionAgmtWithoutOri, geometryBagAgreement);
                    }

                    // insert agreement polygon
                    return this.InsertAgreementSpatial(
                        agreementId,
                        geometryBagAgreement!,
                        this.CalcMapStatus(agreementId),
                        setLowerMapStatus: false,
                        overwriteExisting: !string.IsNullOrEmpty(agreementSpatialId));
                }
            }

            return success;
        }


        //--------------------------------------------------
        [NotNull]
        private IEnumerable<LW_LEGAL_DESCRIPTION> GetLegalsMappedAtHigherLevel(Guid agreementId) =>
            this._dalService
                .GetLegalsForAgreement(agreementId)
                .Where(f => Enum.Parse<MapStatusCodes>(f.MAP_STATUS_CODE) == MapStatusCodes.MappedAtHigherLevel);


        //--------------------------------------------------
        [NotNull]
        private IEnumerable<LW_LEGAL_DESCRIPTION> GetLegalsMappedAtHigherLevel(Guid agreementId, Guid lastTractId) =>
            this._dalService
                .GetLegals(agreementId, lastTractId)
                .Where(f => Enum.Parse<MapStatusCodes>(f.MAP_STATUS_CODE) == MapStatusCodes.MappedAtHigherLevel);


        //--------------------------------------------------
        [NotNull]
        private IEnumerable<LW_TRACT_DETAIL> GetTractsMappedAtHigherLevel(Guid agreementId) =>
            this
                ._dalService
                .GetTractDetailsForAgreement(agreementId)
                .Where(f => Enum.Parse<MapStatusCodes>(f.MAP_STATUS_CODE) == MapStatusCodes.MappedAtHigherLevel);


        //--------------------------------------------------
        /// <summary>
        /// Adjust map status to not mapped MSC=0 where spatial id exist but there is no spatial.
        /// </summary>
        /// <remarks>
        /// This method will reset mapping level from TRACT to LEGAL if there are mapped legals
        /// for mapping level TRACT.
        /// </remarks>
        private void CheckHigherLevelSpatial([NotNull] LW_AGREEMENT_LEVEL agreementLevel)
        {
            using var legalGeodatabase = this._lpmSpatial.LegalLayerConnection.OpenEnterpriseGeodatabase();
            using var legalFeatureClass = legalGeodatabase.OpenFeatureClass(this._lpmSpatial.LegalLayerConnection);

            using var tractGeodatabase = this._lpmSpatial.TractLayerConnection.OpenEnterpriseGeodatabase();
            using var tractFeatureClass = tractGeodatabase.OpenFeatureClass(this._lpmSpatial.TractLayerConnection);


            void ResetLegalsToNotMapped([NotNull] IEnumerable<LW_LEGAL_DESCRIPTION> legalDescriptionRecords)
            {
                foreach (var legal in legalDescriptionRecords)
                {
                    if (legal.SPATIAL_ID is not null
                        && !legalFeatureClass.DoesFeatureExist(legal.SPATIAL_ID))
                    {
                        // reset the legal map status code to not mapped MSC=0
                        // since spatial is missing
                        this._dalService.UpdateLegalSpatialAttributes(
                            legal.LEGAL_DESC_ID,
                            MapStatusCodes.NotMapped,
                            spatialId: null);
                    }
                }
            }

            // set map status code to not mapped for records that
            // have spatial id but no real spatial

            // ReSharper disable once ConvertIfStatementToSwitchStatement
            if (agreementLevel.MAPPING_LEVEL == "LEGAL")
            {
                var tracts = this._dalService.GetTractDetailsForAgreement(agreementLevel.AGMT_ID);

                // handle legals with tract (for each tract in the agreement)
                foreach (var tract in tracts.Where(t => t.TRACT_ID is not null))
                {
                    // 2024-01-09 11:34am trusharm, suppressing warning for null tract
                    // id because they are filtered in foreach loop
                    var tractId = (Guid)tract.TRACT_ID!;
                    var legalsWithTract = this._dalService.GetLegals(tract.AGMT_ID, tractId);

                    ResetLegalsToNotMapped(legalsWithTract);

                    // 2024-01-24 9:30am trusharm, legacy code warning (invalid logic
                    // the "if" block below) - for tract having spatial id (mapped tract)
                    // but zero legals under it, the map status code of the tract
                    // is set to not mapped (MSC=0) when mapping level is LEGAL
                    // - the concern here is that tract polygon is getting orphaned as
                    // because the code is not deleting the tract polygon; the use case
                    // might be rare when user in smart client deletes all legals under the
                    // the tract in question.
                    // we will report this as error for now to see if it happens
                    if (tract.SPATIAL_ID is not null && legalsWithTract.Count == 0)
                    {
                        this._dalService.UpdateTractSpatialAttributes(
                            tract.TRACT_DETAIL_ID,
                            MapStatusCodes.NotMapped,
                            spatialId: null);
                        this._logger.Error(
                            "The tract TRACT_ID='{TractId}' does not contain any legal(s) under it, please review the agreement AGMT_ID='{AgmtId}' as this could be a violation where at least one legal record is required",
                            tractId,
                            agreementLevel.AGMT_ID);
                    }
                }

                // handle legals without tracts (a type of agreement known as contracts)
                var legalsWithoutTract = this
                    ._dalService
                    .GetLegalsForAgreement(agreementLevel.AGMT_ID)
                    .Where(l => l.TRACT_ID is null && l.SPATIAL_ID is not null);

                ResetLegalsToNotMapped(legalsWithoutTract);

                // 2024-01-24 9:30am trusharm, legacy code warning (invalid logic in
                // the "if" block below) - for agreement having spatial id (mapped agreement)
                // but zero tract and legals under it, the map status code of the agreement
                // is set to not mapped (MSC=0) when mapping level is LEGAL
                // - the concern here is that agreement polygon is getting orphaned as
                // because the code is not deleting the agreement polygon; the use case
                // might be rare when user in smart client deletes all tracts legals under the
                // the agreement in question.
                // we will report this as error for now to see if it happens
                if (agreementLevel.SPATIAL_ID is not null
                    && !this._dalService.GetLegalsForAgreement(agreementLevel.AGMT_ID).Any(l => l.SPATIAL_ID is not null)
                    && !this._dalService.GetTractDetailsForAgreement(agreementLevel.AGMT_ID).Any(l => l.SPATIAL_ID is not null))
                {
                    this._dalService.UpdateAgreementSpatialAttributes(
                        agreementLevel.AGMT_ID,
                        MapStatusCodes.NotMapped,
                        spatialId: null);
                    this._logger.Error(
                        "The agreement AGMT_ID=\'{AgmtId}\' does not contain any tract(s) or legal(s) under it, please review the agreement as this could be a violation where at least one tract and one legal record is required for an agreement",
                        agreementLevel.AGMT_ID);
                }
            }
            else if (agreementLevel.MAPPING_LEVEL == "TRACT")
            {
                //  check agreement level
                var tracts = this._dalService.GetTractDetailsForAgreement(agreementLevel.AGMT_ID);
                var codeMapStatuses = this._dalService.GetMapStatuses();

                foreach (var tract in tracts.Where(t => t.TRACT_ID is not null))
                {
                    var tractSpatialExist = tract.SPATIAL_ID is not null
                        && tractFeatureClass.DoesFeatureExist(tract.SPATIAL_ID);

                    // 2024-01-09 11:34am trusharm, suppressing warning for null tract
                    // id because they are filtered in the for each loop
                    var tractId = (Guid)tract.TRACT_ID!;
                    var legalsWithTract = this._dalService.GetLegals(tract.AGMT_ID, tractId);

                    // reset MSC to 0 for legals whose MSC is not suppose to have spatial (hasSpatial flag is N)
                    // or is MSC 15 or legal does not have spatial id
                    var legalsWithTractToBeReset = legalsWithTract
                        .Where(l => Enum.Parse<MapStatusCodes>(l.MAP_STATUS_CODE) == MapStatusCodes.DataChangedCheckMapping
                            || l.SPATIAL_ID is null
                            || codeMapStatuses.Single(r => r.MAP_STATUS_CODE == l.MAP_STATUS_CODE).HAS_SPATIAL.Equals(Enum.GetName(CommonValues.N), StringComparison.OrdinalIgnoreCase));

                    foreach (var legal in legalsWithTractToBeReset)
                    {
                        if (!tractSpatialExist)
                        {
                            this._dalService.UpdateLegalSpatialAttributes(
                                legal.LEGAL_DESC_ID,
                                MapStatusCodes.NotMapped,
                                spatialId: null);

                            // 2024-01-24 9:30am trusharm, legacy code warning (invalid logic)
                            // updating the MSC = 0 would and resetting spatial id without deleting
                            // the polygon will produce orphans for MSC = 15 for mapping level TRACT,
                            // we will simply log this as an error for now to find out if it happens
                            if (legal.SPATIAL_ID is not null)
                            {
                                this._logger.Error(
                                    "The tract TRACT_ID='{TractId}' has spatial but legal qualifies to be set at MSC = 0, please review the agreement AGMT_ID='{AgmtId}'",
                                    tractId,
                                    agreementLevel.AGMT_ID);
                            }
                        }
                        // only set msc to mapped at higher level:
                        //  - if the legal MSC is other than 5, 14 and 15
                        //  - if the legal MSC is 5 or 15 and tract MSC is 1
                        else if ((Enum.Parse<MapStatusCodes>(legal.MAP_STATUS_CODE) != MapStatusCodes.RemarkChangedCheckMapping
                                && Enum.Parse<MapStatusCodes>(legal.MAP_STATUS_CODE) != MapStatusCodes.MappedAtHigherLevel
                                && Enum.Parse<MapStatusCodes>(legal.MAP_STATUS_CODE) != MapStatusCodes.DataChangedCheckMapping)
                            || (Enum.Parse<MapStatusCodes>(legal.MAP_STATUS_CODE)
                                    is MapStatusCodes.RemarkChangedCheckMapping or MapStatusCodes.DataChangedCheckMapping
                                && Enum.Parse<MapStatusCodes>(tract.MAP_STATUS_CODE) == MapStatusCodes.MappedOk))
                        {
                            this._dalService.UpdateLegalSpatialAttributes(
                                legal.LEGAL_DESC_ID,
                                MapStatusCodes.MappedAtHigherLevel,
                                spatialId: null);
                        }
                    }

                    // are there any legals (mapped legals) containing spatial
                    // with any other MSC (with hasSpatial flag set to Y) but 15
                    var mappedLegals = legalsWithTract
                        .Count(l => Enum.Parse<MapStatusCodes>(l.MAP_STATUS_CODE) != MapStatusCodes.DataChangedCheckMapping
                            && l.SPATIAL_ID is not null
                            && codeMapStatuses
                                .Single(r => r.MAP_STATUS_CODE == l.MAP_STATUS_CODE)
                                .HAS_SPATIAL
                                .Equals(Enum.GetName(CommonValues.Y), StringComparison.OrdinalIgnoreCase));
                    if (mappedLegals > 0)
                    {
                        this._logger.Information(
                            "Attempting to reset Mapping level to LEGAL from TRACT for AGMT_ID='{AgmtId}' because there are mapped legals",
                            agreementLevel.AGMT_ID);

                        // 2024-01-19 4:10 trusharm, this logic prevent the use case where more than one legal is
                        // mapped (any MSC with spatial but not MSC=15) at LEGAL level and mapping
                        // level is raised to TRACT
                        // The above use-case needs to be addressed in linker (if user plans to raise mapping level)
                        if (this.UpdateMappingLevel(agreementLevel.AGMT_ID, tractId: null, newMappingLevel: "LEGAL"))
                        {
                            this._logger.Information("Mapping level updated to LEGAL from TRACT for AGMT_ID='{AgmtId}'", agreementLevel.AGMT_ID);
                        }
                        else
                        {
                            this._logger.Error("Unable to update mapping level to LEGAL from TRACT for AGMT_ID='{AgmtId}'", agreementLevel.AGMT_ID);
                        }

                        continue;
                    }


                    if ((tract.SPATIAL_ID is not null || tract.MAP_STATUS_CODE != "0") && !tractSpatialExist)
                    {
                        // tract polygon does not exist then reset tract's map status code to zero
                        this._dalService.UpdateTractSpatialAttributes(
                            tract.TRACT_DETAIL_ID,
                            MapStatusCodes.NotMapped,
                            spatialId: null);
                    }
                }


                // reset agreement spatial for mapping level tract, when there is
                // nothing mapped at tract level
                if (agreementLevel.SPATIAL_ID is not null
                    && !this._dalService.GetTractDetailsForAgreement(agreementLevel.AGMT_ID).Any(l => l.SPATIAL_ID is not null))
                {
                    this._dalService.UpdateAgreementSpatialAttributes(
                        agreementLevel.AGMT_ID,
                        MapStatusCodes.NotMapped,
                        spatialId: null);
                }
            }
        }


        //--------------------------------------------------
        /// <summary>
        /// AutoDeletes the spatial polygon for mapping level if the MSC
        /// qualifies provided the ALLOW_SPATIAL_DELETE in LW_CODE_MAP_STATUS is set to 'Y'.
        /// </summary>
        private bool AutoDeleteSpatial([NotNull] LW_AGREEMENT_LEVEL agreementLevel)
        {
            static bool DoesMapStatusQualifyForReset(
                MapStatusCodes mapStatusCode,
                [NotNull] string mappingLevel,
                [NotNull] string hasSpatial)
            {
                return mappingLevel == "LEGAL"
                    ? !hasSpatial.Equals(Enum.GetName(CommonValues.Y), StringComparison.OrdinalIgnoreCase)
                    && mapStatusCode != MapStatusCodes.CheckRemark
                    // skip the MSC 7 and 11, the are set by AutoMapper
                    // which must not be reset MSC = 0 (Not Mapped)
                    && mapStatusCode != MapStatusCodes.FailedNonStandardLot
                    && mapStatusCode != MapStatusCodes.FailedQuartering
                    && mapStatusCode != MapStatusCodes.MappingIssueNeedsDoc
                    // for mapping level agreement and tract
                    : (mapStatusCode == MapStatusCodes.DataChangedCheckMapping)
                    || (hasSpatial.Equals(Enum.GetName(CommonValues.Y), StringComparison.OrdinalIgnoreCase)
                        && mapStatusCode != MapStatusCodes.CheckRemark
                        && mapStatusCode != MapStatusCodes.RemarkChangedCheckMapping
                        && mapStatusCode != MapStatusCodes.MappingIssueNeedsDoc);
            }

            var success = true;
            var codeMapStatuses = this._dalService.GetMapStatuses();

            var matchingMapStatus = codeMapStatuses.Single(r => r.MAP_STATUS_CODE == agreementLevel.MAP_STATUS_CODE);
            var hasSpatial = matchingMapStatus.HAS_SPATIAL;
            var allowSpatialDelete = matchingMapStatus.ALLOW_SPATIAL_DELETE;

            if (agreementLevel.SPATIAL_ID is null) // agreement without spatial reset map status code
            {
                if (DoesMapStatusQualifyForReset(
                    Enum.Parse<MapStatusCodes>(agreementLevel.MAP_STATUS_CODE),
                    agreementLevel.MAPPING_LEVEL,
                    hasSpatial))
                {
                    success = this._dalService.UpdateAgreementSpatialAttributes(
                        agreementLevel.AGMT_ID,
                        MapStatusCodes.NotMapped,
                        spatialId: null);
                }
            }
            else // agreement with spatial
            {
                // ReSharper disable once ConvertIfStatementToSwitchStatement
                if (hasSpatial.Equals(Enum.GetName(CommonValues.N), StringComparison.OrdinalIgnoreCase)
                    && allowSpatialDelete.Equals(Enum.GetName(CommonValues.Y), StringComparison.OrdinalIgnoreCase))
                {
                    this.DeleteAgreementSpatial(agreementLevel.SPATIAL_ID, setLowerMapStatus: agreementLevel.MAPPING_LEVEL == "AGMT");
                }
                else if (hasSpatial.Equals(Enum.GetName(CommonValues.N), StringComparison.OrdinalIgnoreCase)
                    && allowSpatialDelete.Equals(Enum.GetName(CommonValues.N), StringComparison.OrdinalIgnoreCase))
                {
                    this._logger.Warning(
                        "Unable to delete polygon for qualifying MAP_STATUS_CODE='{MapStatusCode}' and HasSpatial='N' because AllowSpatialDelete='N' for AGMT_ID='{AgmtId}'",
                        agreementLevel.MAP_STATUS_CODE,
                        agreementLevel.AGMT_ID);
                }
            }


            // for mapping level tract or legal (process tracts first)
            var tractDetail = this._dalService.GetTractDetailsForAgreement(agreementLevel.AGMT_ID);
            // tracts without spatial
            foreach (var tract in tractDetail.Where(td => td.SPATIAL_ID is null))
            {
                if (DoesMapStatusQualifyForReset(
                    Enum.Parse<MapStatusCodes>(tract.MAP_STATUS_CODE),
                    agreementLevel.MAPPING_LEVEL,
                    hasSpatial: codeMapStatuses.Single(r => r.MAP_STATUS_CODE == tract.MAP_STATUS_CODE).HAS_SPATIAL))
                {
                    success = this._dalService.UpdateTractSpatialAttributes(
                        tract.TRACT_DETAIL_ID,
                        MapStatusCodes.NotMapped,
                        spatialId: null);
                }
            }
            // tracts with spatial
            foreach (var tract in tractDetail.Where(td => td.SPATIAL_ID is not null))
            {
                matchingMapStatus = codeMapStatuses.Single(r => r.MAP_STATUS_CODE == tract.MAP_STATUS_CODE);

                hasSpatial = matchingMapStatus.HAS_SPATIAL;
                allowSpatialDelete = matchingMapStatus.ALLOW_SPATIAL_DELETE;

                // ReSharper disable once ConvertIfStatementToSwitchStatement
                if (hasSpatial.Equals(Enum.GetName(CommonValues.N), StringComparison.OrdinalIgnoreCase)
                    && allowSpatialDelete.Equals(Enum.GetName(CommonValues.Y), StringComparison.OrdinalIgnoreCase))
                {
                    this._lpmTractSpatial.DeleteTractSpatial(
                        tract.SPATIAL_ID,
                        setLowerMapStatus: agreementLevel.MAPPING_LEVEL == "TRACT",
                        cascadeDelete: false,
                        rollup: true);
                }
                else if (hasSpatial.Equals(Enum.GetName(CommonValues.N), StringComparison.OrdinalIgnoreCase)
                    && allowSpatialDelete.Equals(Enum.GetName(CommonValues.N), StringComparison.OrdinalIgnoreCase))
                {
                    this._logger.Warning(
                        "Unable to delete polygon for qualifying MAP_STATUS_CODE='{MapStatusCode}' and HasSpatial='N' because AllowSpatialDelete='N' for TRACT_DETAIL_ID='{TractDetailId}'",
                        tract.MAP_STATUS_CODE,
                        tract.TRACT_DETAIL_ID);
                }
            }


            // for mapping level legal (process legals)
            if (agreementLevel.MAPPING_LEVEL == "LEGAL")
            {
                var legalDescriptions = this._dalService.GetLegalsForAgreement(agreementLevel.AGMT_ID);

                // legals without spatial
                foreach (var legal in legalDescriptions.Where(l => l.SPATIAL_ID is null))
                {
                    if (DoesMapStatusQualifyForReset(
                        Enum.Parse<MapStatusCodes>(legal.MAP_STATUS_CODE),
                        agreementLevel.MAPPING_LEVEL,
                        hasSpatial: codeMapStatuses.Single(r => r.MAP_STATUS_CODE == legal.MAP_STATUS_CODE).HAS_SPATIAL))
                    {
                        success = this._dalService.UpdateLegalSpatialAttributes(
                            legal.LEGAL_DESC_ID,
                            MapStatusCodes.NotMapped,
                            spatialId: null);
                    }
                }
                // legals with spatial
                foreach (var legal in legalDescriptions.Where(l => l.SPATIAL_ID is not null))
                {
                    matchingMapStatus = codeMapStatuses
                        .SingleOrDefault(r => r.MAP_STATUS_CODE == legal.MAP_STATUS_CODE)
                        .RequireNotNullValue();

                    hasSpatial = matchingMapStatus.HAS_SPATIAL;
                    allowSpatialDelete = matchingMapStatus.ALLOW_SPATIAL_DELETE;

                    // ReSharper disable once ConvertIfStatementToSwitchStatement
                    if (hasSpatial.Equals(Enum.GetName(CommonValues.N), StringComparison.OrdinalIgnoreCase)
                        && allowSpatialDelete.Equals(Enum.GetName(CommonValues.Y), StringComparison.OrdinalIgnoreCase))
                    {
                        this._lpmLegalSpatial.DeleteLegalSpatial(legal.SPATIAL_ID, rollup: true);
                    }
                    else if (hasSpatial.Equals(Enum.GetName(CommonValues.N), StringComparison.OrdinalIgnoreCase)
                        && allowSpatialDelete.Equals(Enum.GetName(CommonValues.N), StringComparison.OrdinalIgnoreCase))
                    {
                        this._logger.Warning(
                            "Unable to delete polygon for qualifying MAP_STATUS_CODE='{MapStatusCode}' and HasSpatial='N' because AllowSpatialDelete='N' for LEGAL_DESC_ID='{LegalDescId}'",
                            legal.MAP_STATUS_CODE,
                            legal.LEGAL_DESC_ID);
                    }
                }
            }

            return success;
        }
