REGISTER datafu-1.2.0.jar
DEFINE CountEach datafu.pig.bags.CountEach();

hearings = LOAD 'default.admin_hearing_results_2012' USING org.apache.hive.hcatalog.pig.HCatLoader();
tickets = LOAD 'default.all_rlc_tickets_2012' USING org.apache.hive.hcatalog.pig.HCatLoader();

joined = JOIN tickets BY ticket_id, hearings BY ticket_id; 

results = FOREACH (GROUP joined BY (camera_address, date)) GENERATE 
    group as camera_by_date, CountEach(joined.(result)) as ticket_count;

DUMP results;