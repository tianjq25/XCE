if [ ! -n "$1" ]; then
    set $1 0.01
fi

echo "Generating data with scale factor $1"
cd TPC-H
rm *.tbl
./dbgen -s $1
cd ..

echo "Transforming data to .table format"
python tools/transform.py