hearings = LOAD 'default.admin_hearing_results_2012' USING org.apache.hive.hcatalog.pig.HCatLoader();
tickets = LOAD 'default.all_rlc_tickets_2012' USING org.apache.hive.hcatalog.pig.HCatLoader();
joined = JOIN tickets BY ticket_id, hearings BY ticket_id; 

liable = FILTER joined BY camera_address == '6200 N LINCOLN AVENUE' AND result == 'Liable';
not_liable = FILTER joined BY camera_address == '6200 N LINCOLN AVENUE' AND result == 'Not Liable';

liable = FOREACH (GROUP liable BY date) GENERATE
   group AS liable_issue_date,
   COUNT(liable) AS liable_cnt;

not_liable = FOREACH (GROUP not_liable BY date) GENERATE
   group as not_liable_issue_date,
   COUNT(not_liable) as not_liable_cnt;

joined = JOIN liable BY liable_issue_date FULL OUTER, not_liable BY not_liable_issue_date;

results = FOREACH joined GENERATE
   liable_issue_date,
   liable_cnt,
   not_liable_cnt,
   ((double)not_liable_cnt / (double)(liable_cnt + not_liable_cnt)) as appeal_success_rate;

ordered = ORDER results BY appeal_success_rate DESC;
dump ordered;