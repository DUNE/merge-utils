# build a submit file

echo "running with tag " $1

metacat query -s "files with merge.tag=$1"

echo "that number better be zero"

python makecommands.py $1

cat FLAT-ES_prod*log | grep pass1 > submit_$1.sh

echo " submit file is submit_$1.sh "
cat submit_$1.sh