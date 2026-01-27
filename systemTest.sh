echo "check justin token"
justin time
justin get-token
echo "---------------------------------------- "
echo "see what the token is"
htdecodetoken 
echo "---------------------------------------- "
# test-paths.sh

echo "which root"
which root
root --version
echo "---------------------------------------- "
echo "which root python version"
root-config --python-version
echo "---------------------------------------- "
echo "which gcc"
which gcc
gcc --version
echo "---------------------------------------- "
echo "which python"
which python
python --version
echo "---------------------------------------- "
echo "which cmake"
which cmake
cmake --version
echo "---------------------------------------- "
which "xrootd"
which xrootd
xrootd -v 
echo "---------------------------------------- "
echo "test metacat"
export dataset="fardet-hd:fardet-hd__trg_mc_2025a_tpg__root-tuple__v10_12_01d01__triggerana_dune10kt_1x2x2__prodmarley_nue_flat_cc_dune10kt_1x2x2__out1__v1_official"

echo "test dataset: $dataset"


export thefile=`metacat query "files from " $dataset " ordered limit 1"`

echo "test file: $thefile"

metacat file show $thefile

echo "---------------------------------------- "
echo "test rucio"
export location=`rucio replica list file $thefile --protocols=root --pfns | head -n1 `

echo "location: $location"
echo "---------------------------------------- "
echo "test root"

export pointer=`root -b -q $location`
echo "pointer: $pointer"

#spack find --loaded



