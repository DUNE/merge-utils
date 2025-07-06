''' Convert a query into a set of files for a dataset definition '''

def Query2DataSet(query=None):
    #print (query)
    local = query.replace("files where","")
    print (local)
    ender = local.split("skip")
    local = ender[0]
    print ("ender",ender)
    skip = False
    limit = False
    if len(ender) > 1:
        skip = ender[1]
        skipfields = skip.split("limit")
        if len(skipfields) > 1: 
            limit = skipfields[1]
        skip = skipfields[0]
    
    fields = local.split("and")
    meta = {}
    if skip: meta["skip"] = skip
    if limit: meta["limit"] = limit
    for field in fields:
        check = field.split("=")
        if len(check)>1:
            name = check[0].strip()
            value = check[1].strip()
            meta[name] = value.replace("'","")
        print (name,value)
    print ("meta",meta)

query = "files where namespace='fardet-vd' and core.application.version=v09_81_00d02 and core.application.name=reco2 and core.data_stream=out1 and core.data_tier='full-reconstructed' and core.file_type=mc and core.run_type='fardet-vd' and dune.campaign=fd_mc_2023a_reco2 and dune.config_file=reco2_dunevd10kt_nu_1x8x6_3view_30deg_geov3.fcl and dune.requestid=ritm1780305 and dune_mc.detector_type='fardet-vd' and dune_mc.gen_fcl_filename=prodgenie_nu_dunevd10kt_1x8x6_3view_30deg.fcl and dune.output_status=confirmed and core.group=dune"

Query2DataSet(query)