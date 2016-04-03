# mapreduce-apps

##*tag_counter* module
###Description
Count amount of all the tags in dataset. Output files contains data with rows as TAG,COUNT
###Run instructions
* build module with maven
* copy file src/test/resources/user.profile.tags.us to target dir (dir with result jar)
* copy dataset to hdfs to <hdfs-input-data> folder
* remove output folder <hdfs-output-data> if exists
* run app with
```bash
yarn jar tag_counter-1.0.jar -files user.profile.tags.us <hdfs-input-data> <hdfs-output-data>
```

##*visit_counter* module
###Description
Count amount of visits by IP and spends. Output is a sequence file compressed with Snappy codec and contains data with IP
for key and (VISIT_COUNT,SPENDS) pair for value
###Run instructions
* build module with maven
* copy dataset to hdfs to <hdfs-input-data> folder
* remove output folder <hdfs-output-data> if exists
* run app with
```bash
yarn jar visit_counter-1.0.jar -D mapreduce.output.fileoutputformat.compress=true 
-D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.Snappy <hdfs-input-data> <hdfs-output-data>
```

##*second_sort* module
###Description
Sort dataset by iPinYou Id values, then by timestamp and count events of site-impression type. 
Also write to std out max site-impression counter. Output contains rows with IPINYOU_ID,TIMESTAMP,SITEIMPR_COUNTER
###Run instructions
* build module with maven
* copy dataset to hdfs to <hdfs-input-data> folder
* remove output folder <hdfs-output-data> if exists
* run app with
```bash
yarn jar second_sort-1.0.jar <hdfs-input-data> <hdfs-output-data>
```
* MAX counter can be find in application log. Line have the following format: *MAX: IPinYouId - ${id} count - ${max_counter}*

