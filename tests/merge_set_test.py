"""Tests for the metacat utils module"""

import pytest
from merge_utils import config
from merge_utils.merge_set import MergeFile, MergeFileError, MergeSet

config.load()  # Load the default configuration for testing

FILE_DEFAULTS = {
    "namespace": "fardet-hd",
    "name": "anu_dune10kt_1x2x6_70520830_0_20230721T123554Z_gen_g4_detsim_hitreco.root",
    "created_timestamp": 1690562373.966872,
    "creator": "dunepro",
    "fid": "79671614",
    "size": 26731046,
    "checksums": {
        "adler32": "489d301a"
    },
    "metadata": {
        "DUNE.campaign": "fdhdvd_ritm1780305",
        "DUNE.requestid": "ritm1780305",
        "art.file_format_era": "ART_2011a",
        "art.file_format_version": 15,
        "art.first_event": 1,
        "art.last_event": 1,
        "art.process_name": "Reco1",
        "art.run_type": "fardet-hd",
        "core.application": "art.reco",
        "core.application.family": "art",
        "core.application.name": "reco",
        "core.application.version": "v09_75_03d00",
        "core.data_stream": "out1",
        "core.data_tier": "hit-reconstructed",
        "core.end_time": 1689943892.0,
        "core.event_count": 1,
        "core.file_format": "artroot",
        "core.file_type": "mc",
        "core.first_event_number": 1,
        "core.group": "dune",
        "core.last_event_number": 1,
        "core.run_type": "fardet-hd",
        "core.runs": [
            70520830
        ],
        "core.runs_subruns": [
            7052083000001
        ],
        "core.start_time": 1689943888.0,
        "dune_mc.beam_flux_ID": "1",
        "dune_mc.beam_polarity": "RHC",
        "dune_mc.detector_type": "fardet-hd",
        "dune_mc.electron_lifetime": "10.4",
        "dune_mc.generators": "genie ",
        "dune_mc.generators_version": "3.04_00c",
        "dune_mc.geometry_version": "dune_10kt_v4_refactored_1x2x6.gdml",
        "dune_mc.liquid_flow": "no",
        "dune_mc.mixerconfig": "mixed",
        "dune_mc.space_charge": "no",
        "dune_mc.with_cosmics": "0"
    },
    "retired": False,
    "retired_by": None,
    "retired_timestamp": None,
    "updated_by": None,
    "updated_timestamp": 1690562373.966872
}

def file_dict(init_dict = None):
    """Create a file dictionary for testing"""
    if init_dict is None:
        init_dict = {}
    for key, value in FILE_DEFAULTS.items():
        if key not in init_dict:
            init_dict[key] = value
        elif init_dict[key] is None:
            del init_dict[key]
        elif isinstance(value, dict):
            for subkey, subvalue in value.items():
                if subkey not in init_dict[key]:
                    init_dict[key][subkey] = subvalue
                elif init_dict[key][subkey] is None:
                    del init_dict[key][subkey]
    return init_dict

@pytest.mark.parametrize("spec, errors", [
    ({
        'namespace': 'ns1',
        'name': 'file1',
        'size': 100,
        'metadata': {
            'field1': 'value1',
            'field2': 'value2'
        }
    }, MergeFileError.INVALID),
    ({
        'namespace': 'ns2',
        'name': 'file2',
        'size': 200,
        'metadata': {
            'field3': 'value3'
        }
    }, MergeFileError.INVALID),
    ({
        'namespace': 'ns3',
        'name': 'file3',
        'size': 200,
        'fid': None
    }, MergeFileError.UNDECLARED)
])
def test_merge_file(spec, errors):
    """Test the MergeFile class behavior"""
    f_dict = file_dict(spec)
    f_obj = MergeFile(f_dict)
    assert f_obj.did == f_dict['namespace'] + ':' + f_dict['name']
    assert f_obj.namespace == f_dict['namespace']
    assert f_obj.name == f_dict['name']
    assert f_obj.size == f_dict['size']
    assert hash(f_obj) == hash(f_dict['namespace'] + ':' + f_dict['name'])
    assert str(f_obj) == f_dict['namespace'] + ':' + f_dict['name']
    for field, value in f_dict['metadata'].items():
        if isinstance(value, list):
            value = str(value)
        assert f_obj.get_fields([field]) == (f_dict['namespace'], value)
    assert f_obj.errors == errors
