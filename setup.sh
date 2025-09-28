export MERGE_UTILS_DIR="$(dirname `readlink -f "${BASH_SOURCE[0]}"`)"
echo "Setting MERGE_UTILS_DIR to $MERGE_UTILS_DIR"

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
    setup dunesw v10_08_01d00 -q e26:prof

    export METACAT_AUTH_SERVER_URL=https://metacat.fnal.gov:8143/auth/dune
    export METACAT_SERVER_URL=https://metacat.fnal.gov:9443/dune_meta_prod/app 
    setup metacat

    setup rucio
    # Try to fix broken rucio config file
    mkdir -p $MERGE_UTILS_DIR/config/misc/
    export RUCIO_CONFIG=$MERGE_UTILS_DIR/config/misc/rucio.cfg
    cp $RUCIO_HOME/etc/rucio.cfg $RUCIO_CONFIG
    sed -i 's/account = .*/account = '$USER'/' $RUCIO_CONFIG
    sed -i 's/auth_type = .*/auth_type = oidc/' $RUCIO_CONFIG
    echo "oidc_scope = openid profile email org.cilogon.userinfo wlcg.capabilityset:/duneana wlcg.groups:/dune" >> $RUCIO_CONFIG

    setup justin
    htgettoken -a htvaultprod.fnal.gov -i dune

    python3 -m venv $MERGE_UTILS_DIR/.venv_sl7
    . $MERGE_UTILS_DIR/.venv_sl7/bin/activate
    pip install $MERGE_UTILS_DIR --use-feature=in-tree-build

    pip install h5py
    
fi
