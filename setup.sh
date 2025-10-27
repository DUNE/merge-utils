export DUNE_VERSION=${DUNE_VERSION:-v10_12_01d00}
export DUNE_QUALIFIER=${DUNE_QUALIFIER:-e26:prof}

export MERGE_UTILS_DIR="$(dirname `readlink -f "${BASH_SOURCE[0]}"`)"
echo "Setting MERGE_UTILS_DIR to $MERGE_UTILS_DIR"

# Set up rucio configuration file
mkdir -p $MERGE_UTILS_DIR/config/misc/
export RUCIO_CONFIG=$MERGE_UTILS_DIR/config/misc/rucio.cfg
sed "s/<username>/$USER/g" $MERGE_UTILS_DIR/config/misc/rucio_template.cfg > $RUCIO_CONFIG

release=`lsb_release -i | cut -f 2`
if [[ "$release" == "AlmaLinux" ]]; then
    echo "Doing setup for Alma Linux"

    source /cvmfs/larsoft.opensciencegrid.org/spack-packages/setup-env.sh
    spack load root@6.28.06
    spack load r-m-dd-config experiment=dune
    spack load justin
    htgettoken -a htvaultprod.fnal.gov -i dune

    spack load hdf5
    spack load py-h5py

    python3 -m venv $MERGE_UTILS_DIR/.venv_al9
    . $MERGE_UTILS_DIR/.venv_al9/bin/activate
    pip install --upgrade pip
    pip install --editable "$MERGE_UTILS_DIR[test]"

elif [[ "$release" == "Scientific" ]]; then
    echo "Doing setup for Scientific Linux"

    export UPS_OVERRIDE="-H Linux64bit+3.10-2.17"
    source /cvmfs/dune.opensciencegrid.org/products/dune/setup_dune.sh
    setup dunesw $DUNE_VERSION -q $DUNE_QUALIFIER

    export METACAT_AUTH_SERVER_URL=https://metacat.fnal.gov:8143/auth/dune
    export METACAT_SERVER_URL=https://metacat.fnal.gov:9443/dune_meta_prod/app 
    setup metacat

    setup rucio

    setup justin
    htgettoken -a htvaultprod.fnal.gov -i dune

    python3 -m venv $MERGE_UTILS_DIR/.venv_sl7
    . $MERGE_UTILS_DIR/.venv_sl7/bin/activate
    pip install $MERGE_UTILS_DIR --use-feature=in-tree-build

    pip install h5py
    
fi
