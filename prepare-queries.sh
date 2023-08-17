for i in `ls ./query_templates/query*tpl`
do 
    echo $i;  
    echo "define _END = \"\";" >> $i
done
