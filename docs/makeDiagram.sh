export TEMP=$PWD
export HERE=$PWD/..
cd $HERE/src/merge_utils
echo $PWD
pydeps metacat_utils.py -T png --include-missing --cluster --max-bacon 2 --rankdir BT
pydeps io_utils.py -T png --include-missing --cluster --max-bacon 2 --rankdir BT
pydeps merge_set.py -T png --include-missing --cluster --max-bacon 2 --rankdir BT
pydeps merge_rse.py -T png --include-missing --cluster --max-bacon 2 --rankdir BT
pydeps rucio_utils.py -T png --include-missing --cluster --max-bacon 2 --rankdir BT
pydeps justin_utils.py -T png --include-missing --cluster --max-bacon 2 --rankdir BT
pydeps merge_utils.py -T png --include-missing --cluster --max-bacon 2 --rankdir BT
#mv file_utils.png $HERE/docs
#mv rucio_utils.png $HERE/docs
chmod +x *.png
mv *.png $HERE/docs/source

cd $TEMP
echo $PWD
