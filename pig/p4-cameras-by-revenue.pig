tickets = LOAD 'default.all_rlc_tickets_2012' USING org.apache.hcatalog.pig.HCatLoader();

cameras = GROUP tickets BY camera_address;

results = FOREACH cameras GENERATE 
    group as camera_address, 
    COUNT(tickets) as ticket_count, 
    COUNT(tickets) * 100 as revenue;

ordered_results = ORDER results BY revenue DESC;

DUMP ordered_results;