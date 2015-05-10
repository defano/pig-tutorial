REGISTER datafu-1.2.0.jar
DEFINE Quantile datafu.pig.stats.StreamingQuantile('0.99');

tickets = LOAD 'default.all_rlc_tickets_2012' USING org.apache.hcatalog.pig.HCatLoader();

tickets_by_address_date = FOREACH (GROUP tickets BY (camera_address, date)) GENERATE 
    group.camera_address AS camera_address, 
    group.date AS date, 
    COUNT(tickets) AS ticket_count;    

quantiles_by_address = FOREACH (GROUP tickets_by_address_date BY (camera_address)) GENERATE
    group AS camera_address, 
    Quantile(tickets_by_address_date.ticket_count) AS quantile_99,
    AVG(tickets_by_address_date.ticket_count) AS average;

tickets_quantiles = JOIN tickets_by_address_date BY camera_address, quantiles_by_address BY camera_address;
outliers = FILTER tickets_quantiles BY (ticket_count > quantile_99.($0));

DUMP outliers; 