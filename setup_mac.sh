export DUNE_VERSION=${DUNE_VERSION:-v10_12_01d01}
export DUNE_QUALIFIER=${DUNE_QUALIFIER:-e26:prof}

if [[ "$OSTYPE" == "darwin"* ]]; then
    release="MacOS"
    echo "Doing setup for MacOS"
else
    #release=`lsb_release -i | cut -f 2`
    echo "Unknown operating system, unable to run setup!"
    exit 1
fi

if [ -n "$ZSH_VERSION" ]; then
    SCRIPT_PATH="${(%):-%x}"
elif [ -n "$BASH_SOURCE" ]; then
    SCRIPT_PATH="${BASH_SOURCE[0]}"
else
    SCRIPT_PATH="$0"
fi
export MERGE_UTILS_DIR="$(dirname "$(realpath "$SCRIPT_PATH")")"
echo "Setting MERGE_UTILS_DIR to $MERGE_UTILS_DIR"

# Set up rucio configuration file
mkdir -p $MERGE_UTILS_DIR/config/rucio/
export RUCIO_CONFIG=$MERGE_UTILS_DIR/config/rucio/rucio.cfg
sed "s/<username>/$USER/g" $MERGE_UTILS_DIR/config/rucio/template_mac.cfg > $RUCIO_CONFIG

# Build and activate the virtual environment
uv --directory $MERGE_UTILS_DIR sync
source $MERGE_UTILS_DIR/.venv/bin/activate

# Set up servers and authentication
export METACAT_AUTH_SERVER_URL=https://metacat.fnal.gov:8143/auth/dune
export METACAT_SERVER_URL=https://metacat.fnal.gov:9443/dune_meta_prod/app
htgettoken -a htvaultprod.fnal.gov -i dune