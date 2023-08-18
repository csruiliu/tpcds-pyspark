#!/bin/bash

TEMPLATEDIR="/home/ruiliu/Develop/tpcds-pyspark/query_templates"
TOOLSDIR="/home/ruiliu/Develop/tpcds-pyspark/tools"
OUTDIR="/home/ruiliu/Develop/tpcds-pyspark/queries"
SCALE=1
MV_OPTION=" -f "

for i in {1..99}
do
    echo "Generating Query $i"
    cmd="echo query${i}.tpl > $TEMPLATEDIR/templates_${i}.lst"
    eval "$cmd"

    cmd="$TOOLSDIR/dsqgen -DIRECTORY $TEMPLATEDIR -INPUT $TEMPLATEDIR/templates_${i}.lst -DISTRIBUTIONS $TOOLSDIR/tpcds.idx -VERBOSE Y -QUALIFY Y -SCALE $SCALE -DIALECT netezza -OUTPUT_DIR $OUTDIR"
    eval "$cmd"

    #cmd="mv $MV_OPTION $OUTDIR/query_0.sql $OUTDIR/query${i}.sql"
    cmd="mv $MV_OPTION $OUTDIR/query_0.sql $OUTDIR/q${i}.py"
    eval "$cmd"

    sed -i '1s/^/query = \"\"\"/' "q{$i}.py"
    sed -i -e '$a\"\"\"' "q{$i}.py"
done