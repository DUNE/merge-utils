import os, sys

from metacat.webapi import MetaCatClient

mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])



def am_i_done(did=None,descrip=None,DEBUG=False):
    topmeta = mc_client.get_file(did=did, with_metadata=True, with_provenance=True)
    if len(topmeta["children"])==0:
        print("No children found for ",did)
        return False
    found = False
    for file in topmeta["children"]:
        meta = mc_client.get_file(fid=file['fid'], with_metadata=True)
        cdid = meta["namespace"] + ":" + meta["name"]
        metadata = meta["metadata"]
        local = True
        for key,value in descrip.items():
            if key is "namespace":
                if str(meta[key]) != str(value):
                    if DEBUG: print(f"Value mismatch for key {key} in file {cdid}: expected {value}, found {meta['key']}")
                    local = False
                continue
            if key not in metadata:
                if DEBUG: print(f"Key {key} not found in metadata of file {cdid}")
                local = False
            if str(metadata[key]) != str(value):
                if DEBUG: print(f"Value mismatch for key {key} in file {cdid}: expected {value}, found {metadata[key]}")
                local = False
        if local:
            if DEBUG: print(f"There are children of {did} that match the description {descrip}")
            return local
    return found

if __name__ == "__main__":
    testdid = "fardet-hd:prodmarley_nue_flat_es_dune10kt_1x2x2_20250926T170532Z_gen_001914_supernova_g4_detsim_20251113T074432Z_reco.root"
    testfields = {
        "merge.tag": "FLAT-ES_prod_v2-pass2",
        "core.run_type":"fardet-hd"
    }
    test = am_i_done(did=testdid, descrip=testfields)
    print("Am I done?", test) 
