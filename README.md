Chicago Coders Conference: Big Data Hands on Lab
================================================

In this hands-on lab, we'll be duplicating research performed by investigative reporters at the Chicago Tribune as part of their [award-winning series on red light cameras in Chicago](http://www.chicagotribune.com/news/watchdog/redlight/). In case you're unfamiliar with this research, the Tribune series discovered that:

*	Select red-light cameras seemed to go on ticket-issuing benders. All of a sudden, affected cameras began nabbing drivers at a rate in excess of fifty times their historical average.

*	Such spikes were often preceded and succeeded by periods of no activity, suggesting perhaps that cameras were reconfigured by the operator without mandated notice or documentation.

*	Drivers that appeal red-light tickets typically win their cases 10% of the time, but tickets issued during these spikes were overturned 45% of the time. 

Specifically, we'll be reproducing the data found [in this article](http://apps.chicagotribune.com/news/local/red-light-camera-tickets/) illustrating periods of abnormally high ticketing activity and identifying any correlation to periods of appeal success. 

Getting Started
---------------

We'll be using the Hortonworks Sandbox v2.1 as our "big data" platform during this exercise. The sandbox is freely available, installs trivially, and provides a user friendly interface. It even has its own built-in tutorial which you can use to improve your skill-set outside this course. 

Most host machines should have little trouble running the sandbox, but official requirements from Hortonworks state:

* Windows XP, Windows 7, Windows 8 or Mac OS X
* Minimum 4GB RAM; 8GB required to run Ambari and Hbase
* Virtualization enabled on BIOS
* Browser: Chrome 25+, IE 9+, Safari 6+ recommended. (Sandbox will not run on IE 10)

#### A quick refresher on terminology

Recall that Pig deals with data in the form of _relations_, _bags_, _tuples_ and _fields_:

* A **field** is a typed data element, like `City of Chicago` (a `chararray`), `2012` (an `int`), or `3.1415` (a `double`).
* A **tuple** is an ordered set of fields notated with parentheses, like `(1, 2, 3)` or `(pi, 3.1415)`. Analogous to a row or a record in a database.
* A **bag** is an unordered collection of tuples notated with braces, like `{(a, b), (1, 2)}`. Analogous to a table in a database.
* A **relation** is an outer bag. Given that bags can contain other bags (tuples can also contain other tuples) we call the outer-most bag the relation. Analogous to a database... sort of. 

These relational database analogies fall apart quickly because, unlike a relational database, Pig supports bags inside of bags, the equivalent of embedding a table--or an entire database of tables--in a single field. 

Each collection type has a schema associated with it. A schema binds a type and an alias (a name) to each element. Consider this example schema:

```
results: {dates:tuple(start_year:int,end_year:int),ages:tuple(name:chararray,age:int)}
```

From left to right:

* `results` is the alias (the name) of the relation
* `dates:tuple(...)` indicates that the relation contains a tuple called `dates`
* `(start_year:int, end_year:int)` denotes that the `dates` tuple contains two integers, called `start_year` and `end_year` respectively. 
* `ages:tuple(name:chararray,age:int)` indicates the relation also contains a second tuple called `ages` comprised of a string (`chararray`) called `name` and an integer called `age`.

Note that you may find some elements in your schema are preceded with an an identifier and two colons, for example, `tickets::ticket_count:int`. This identifier (`tickets::`) indicates the element's namespace which is set equal to the alias from which it originated when joining or generating derived relations. Namespaces--as we'll see in the exercises--are important when differentiating between elements that would otherwise have the same alias.

You can print the schema of any alias inside a script with the command: `DESCRIBE alias;` or better yet, let Pig diagram the table (in ASCII art) for you using the `ILLUSTRATE alias;` command. As I'm working on a script I often find it helpful to `DESCRIBE` each alias as I create it. This makes it easy to see how Pig has identified elements in a relation, especially when `JOIN` operations produce complex, namespaced schema aliases. 

### Part 1: Install and run the Hortonworks virtual machine:

1. Locate the VirtualBox software on the provided thumb drive (or download it [from the VirtualBox website](https://www.virtualbox.org/wiki/Downloads)) and install it on your Mac or PC. 

2. Locate the Hortonwork Sandbox on the provided thumb drive (or download it [from the Hortonworks website](http://hortonworks.com/products/hortonworks-sandbox/#install)) and import the virtual machine into VirtualBox by double-clicking the `.ova` file . Note that the import process may take several minutes. 

3. Once imported, click the "Start" button to run the virtual machine. 
   
4. The Hortonworks sandbox runs a custom web app called Hue that we'll be interacting with inside the browser. As soon as the virtual machine has booted, its console will display the URL at which you can access the application. Open a browser and navigate to that URL, [http://127.0.0.1:8888](http://127.0.0.1:8888). 

 **If you'd rather skip the registration process**, you may jump directly to the Hue application at [http://localhost:8000](http://localhost:8000). (If prompted, log in with username `hue` and password `1111`.)

Importing the Ticket Data
-------------------------

All of the data required for this exercise can be found on the USB (under the `chicago-rlc-data` directory), or [on Github](https://github.com/defano/ccc-big-data/blob/master/chicago-rlc-data.zip). 

As is the case with virtually every introduction to big data, the example dataset we'll use in this course isn't big enough to really be considered "big data". Nonetheless, the tools and techniques we employ are scalable to datasets thousands of times the size of our paltry quarter-gig red light ticket log. That said, these big data tools are designed to scale; not to perform well in single-node, virtualized environments like ours. Running these analyses on the 250MB dataset will take quite some time. To reduce this write-run-debug cycle time, we've included subset data for the year 2012. But even with this subset, some of these scripts will take five minutes or more to execute). 

Of course, the following instructions will work equally well with the full dataset. The interested student is encouraged to repeat these exercises using the full set of red light camera data after succeeding with the 2012 subset.

### Part 2: Import the ticket records

We'll begin by using Apache's HCatalog to import and store our data on the platform. HCatalog provides a unified, relational view of the data stored on disk and which may happen to be represented in a variety of formats (like CSV or JSON). HCatalog abstracts the formatting details of the underlying data such that data analysis tools higher up the stack--like Pig, Hive or MapReduce--can operate on the data without concern for how its structured or formatted.

1. On the button bar at the top of the page, click the "HCat" icon. You'll be presented with the "HCatalog: Table List" page.

2. Click the "Create a new table from a file" link under in the actions panel on the left. As you do this, take note of the selected database (`default`); this is the database in which our table will be created. 

3. Name the table `rlc_all_tickets_2012` and provide a short description, something like `All red light tickets in 2012`. The table name will matter in future steps; the description will not.

4. Click the "Choose a file" button adjacent to the input file field. In the modal dialog that appears, click the "Upload a file" button, then navigate to and choose the `all_rlc_tickets_2012.txt` file on your filesystem.

5. As soon as the file has uploaded it will appear in the list of available files; click its link to select it.

6. The page will now display additional file import options and a preview of the table that will be created. HCatalog will auto-detect most of the file structure options for us (like encoding and delimiters). Leave the default selections as they are. 

7. In the "Table Preview" section of the page we find the HCatalog has inferred data types for each column, but we  need to provide more meaningful column names:
  - Change the first column name from `7003809063` to `ticket_id`
  - Change the second column name from `2007_01_01` to `date`
  - Change the third column name from `x719312` to `license_plate_number`
  - Change the fourth column name from `pas` to `license_plate_type`
  - Change the fifth column name from `il` to `license_plate_state`
  - Change the sixth column name from `4000_w_chicago_avenue` to `camera_address`
  
  **It's critically important** that you use the same column names provided here if you plan on using the examples in this lab verbatim. If you make up your own column names, you'll need to adjust the example scripts accordingly.

8. Click the "Create Table" button. The operation may take a few minutes to complete. When the creation process has finished, the page will return to "HCatalog: Table List" and you'll note the presence of our newly created table, `all_rlc_tickets_2012`, in the list. 
  
Congratulations! You've just created your first HCatalog table!

### Part 3: Import the appeal records

We'll create a second table representing each ticket appeal attempt following the same steps we used for the ticket records:

1. Click the "Create a new table from a file" link under in the actions panel on the left.

2. Name the table `admin_hearing_results_2012` and provide a short description.

3. Click the "Choose a file" button, then "Upload a file". Find and select the `admin_hearing_results_2012.txt` file on your filesystem.

4. Once the upload completes, click the table to display the file import options; leave the default selections as they are, but rename the columns accordingly:
  - Change the first column to `ticket_id`
  - Change the second column to `issue_date`
  - Change the third column to `hearing_date`
  - Change the fourth column name `result`

  **Don't make life more difficult**, stick with the column names shown here. 

5. Click the "Create Table" button and wait a few minutes while the table is created.

Analyzing the Data
------------------

In this tutorial, we'll be using Apache Pig to crunch our data. Pig is a scripting language that enables data scientists to analyze datasets using a reasonably simple scripting language (called, no less, *Pig Latin*) without regard to the reasonably complex, underlying map-reduce architecture. Pig compiles Pig Latin scripts into one or more map-reduce jobs that execute in the Hadoop environment. Think of map-reduce as Big Data's assembly language and Pig Latin as Big Data's C.

### Part 4: Determine which cameras generate the most revenue

We'll start with a fairly simple task: For each camera (identified by its address), count the number of tickets issued by that camera in 2012. 

The algorithm we'll follow for doing so is:

* Load the ticket records
* Group the tuples based on camera address
* For each group, count the number of tickets issued
* Output a list (*bag*) of tuples consisting of `(camera address, ticket count)`

_**A note to the pedantic:** Clearly, each intersection has multiple cameras installed to capture traffic moving in different directions. In the context of this lab the term "camera" will refer logically to all the physical cameras installed at a given intersection._

1. Start by clicking the "Pig" icon in the button bar at the top of the page. 
2. Give our script a name by entering something like `CountTickets` into the `Title` field.
3. In the script editor field, enter the following statement to tell Pig to load the `all_rlc_tickets_2012` dataset into a relation called `tickets` (its *alias*). 

        tickets = LOAD 'default.all_rlc_tickets_2012' USING org.apache.hcatalog.pig.HCatLoader();

  You may also find it helpful to use the "Pig Helper" drop-down menu to automatically populate the right-hand side of this expression (look under the "HCatalog" submenu).

4. Our first transformation on the `tickets` data will be to group the records (tuples) by camera address. The goal is to produce a relation (bag) in which each tuple in the bag contains two fields: the camera address aliased as `group` and each corresponding record aliased with the original relation's name (`tickets`, in our case). Enter this statement after the previous line in your script: 

        cameras = GROUP tickets BY camera_address;

  The result of this will be to produce a new relation with the schema `cameras: {group:chararray, tickets:{...}}`.

5. For each group (i.e., for each different red light camera in Chicago), produce a bag containing the camera address, the number of tickets issued, and the number of tickets issued multiplied by the fine for each ticket ($100).

        results = FOREACH cameras GENERATE 
	        group AS camera_address, 
	        COUNT(tickets) AS ticket_count, 
	        COUNT(tickets) * 100 AS revenue;

 A few notes about what's happening here:
 * This statement will produce a `results` alias that should contain a relation with the schema `{camera_address:chararray, ticket_count:int, revenue:int}`
 * For completeness and illustration, we're assigning field names (i.e., `AS camera_address`, `AS ticket_count`, `AS revenue`) to the data we're generating in the resulting relation. Since we don't need to refer to these fields in the future, this is isn't necessary; the `as...` clauses could be removed without harm. 
 * The `COUNT` operator returns the number of non-null elements in the specified bag. The intent here is to report the number of rows/elements in the group. 
 
6. Now, lets order the `results` bag by `revenue` so that we can quickly identify those cameras producing the greatest revenue in descending (`DESC`) order:

        ordered_results = ORDER results BY revenue DESC;
        
7. Finally, dump our `ordered_results` relation to output:

        DUMP ordered_results;

8. Check your results. Your finished Pig script should look like:

        tickets = LOAD 'default.all_rlc_tickets_2012' USING org.apache.hcatalog.pig.HCatLoader();
        
        cameras = GROUP tickets BY camera_address;
        
        results = FOREACH cameras GENERATE 
            group as camera_address, 
            COUNT(tickets) as ticket_count, 
            COUNT(tickets) * 100 as revenue;
        
        ordered_results = ORDER results BY revenue DESC;
        
        DUMP ordered_results;

### Part 5: Execute your very first Pig script

First and foremost: Check, double-check, and triple-check your script for errors and typos. It can take Pig a few minutes to catch obvious syntax errors. Having to wait several minutes to figure out you forgot a semicolon is painful! Worse yet, semantic errors (like referencing a nonexistent field in a relation) won't be caught until the statement executes at runtime. With sizable datasets this could be hours from now! When programming Pig, it's well worth the time investment to review your code before you run it. 

When you're sure your script looks good:

1. Click "Execute" to submit the job.

2. As the job runs, note the presence of the blue progress bar that appears directly below the script editor field.
  - For a more detailed view of whats going on, open the "Job Browser" (the construction hard-hat icon) in a new tab. You'll note that by executing your script, a "TempletonControllerJob" has started on the platform. This controller job manages your script execution and is responsible for starting the child map-reduce jobs resulting from operations specified in your script.
  - If you close the script editor window or wish to see results from previously executed jobs, click the "Query History" link at the top of the Pig page. 

3. Your script will probably take 2-5 minutes to execute depending on your machine's horsepower. Provided your script executed successfully, the output (which will appear below the script editor field) should be:

        (4200 S CICERO AVENUE,19800,1980000)
        (400 W BELMONT AVE,15076,1507600)
        (30 W 87TH STREET,12376,1237600)
        (400 S WESTERN AVENUE,12081,1208100)
        ...

It should be obvious that the first element in the tuple is the camera address (i.e., `4200 S CICERO AVENUE`); the second element is the number of tickets issued (`19800`) and the third element is the total revenue collected by the city (assuming a $100 fine with no tickets overturned in court). 

Congratulations on completing your first Pig Latin script!

### Part 6: Find cameras and dates that issued an abnormally large number of tickets

Next, lets see if we can identify dates on which camera's issued an abnormally large number of tickets. For the purposes of this exercise, we're going to define _abnormally large_ as any date on which the number of tickets issued by a camera is in the 99th percentile of tickets issued. That is, fewer than 1% of all dates produce more tickets for the given camera.

Here's the process we'll follow:

* Count the number of tickets issued per camera, per date
* Determine how many tickets constitute the 99th percentile for each camera
* Join the 99th percentile data with the number of tickets per camera per day
* Filter the result, eliminating records where the number of tickets issued is fewer than the 99th percentile. 

We'll be making use of a _user defined function_, (UDF) library called "DataFu" that was created by LinkedIn to simplify the calculation of percentiles/quantiles. UDFs provide an extension to the Pig Latin language and can be written in Java, Python and Javascript. While authoring UDFs is relatively straightforward, the details are beyond the scope of this lab.

1. Create a new script by clicking the "New Script" link on the page. Give the new script a name like `Outliers`.

2. Upload the DataFu UDF to the Hortonworks platform by clicking the "Upload UDF Jar" button. Locate the `datafu-1.2.0.jar` library under the `lib` directory of the USB (or on GitHub, [here](https://github.com/defano/ccc-big-data/blob/master/lib/datafu-1.2.0.jar)).

3. In order to make use of this library inside our Pig Latin script, we need to tell Pig about it, and, as a convenience, assign an alias to the function's name (so as not to have to refer to the method using its fully-qualified package name):

        REGISTER datafu-1.2.0.jar
        DEFINE Quantile datafu.pig.stats.StreamingQuantile('0.99');

  You might be wondering:
  * **What's the `.99` argument?** The `'0.99'` is a constructor argument passed to the UDF indicating that we wish to calculate only the 99th percentile. Alternately, DataFu could calculate a list of different quantiles simultaniously with an input like `('.25', '.50', '.75')`. 
  * **How did you know the constructor syntax and package name?** By reading [the documentation](http://datafu.incubator.apache.org/docs/datafu/guide/bag-operations.html), of course. 
        
4. Like our last script, we only need to load the ticket data:

        tickets = LOAD 'default.all_rlc_tickets_2012' USING org.apache.hcatalog.pig.HCatLoader();
        
5. Our first transformation on `tickets` will be to group all records (tuples) by camera address and date such that each group represents the activity of a given camera on a given date. To determine the number of tickets issued by `(camera_address, date)` date we simply count number of elements in the group:

        tickets_by_address_date = FOREACH (GROUP tickets BY (camera_address, date)) GENERATE 
        	group.camera_address AS camera_address, 
            group.date AS date, 
            COUNT(tickets) AS ticket_count;    

6. Now that we've produced a count of tickets issued by each camera on each day of the year, we need some statistical calculations to determine how many tickets in one day would be considered the 99th percentile. We'll use the DataFu `StreamingQuantile` UDF to help with this. For good measure, we're also calculating the average number of tickets issued per day by the camera (using the built-in `AVG` function):

        quantiles_by_address = FOREACH (GROUP tickets_by_address_date BY (camera_address)) GENERATE
        	group AS camera_address, 
            Quantile(tickets_by_address_date.ticket_count) AS quantile_99,
            AVG(tickets_by_address_date.ticket_count) AS average;

  Note that when we group `tickets_by_address_date BY camera_address`, the resulting relation has two elements: the camera address aliased as `group` and the associated grouped rows (tuples) in a bag aliased with the name of the originating relation, `tickets_by_address_date`. This is why can't simply refer to `ticket_count`; we have to reach inside the bag that contains it using the `tickets_by_address_date.ticket_count` syntax.

7. At this point, we have two tables: 
  - `tickets_by_address_date` with the schema `{camera_adddress:chararray, date:chararray, ticket_count:int}`, and
  - `quantiles_by_address` with the schema `{camera_address:chararray, quantile_99:double, average:double}`
  
  Lets perform an inner-join on these tables by `camera_address` to produce `{camera_adddress:chararray, date:chararray, ticket_count:int, camera_address:chararray, quantile_99:double, average:double}` using the `JOIN` statement:
  
        tickets_quantiles = JOIN tickets_by_address_date BY camera_address, quantiles_by_address BY camera_address;

8. Reduce the result set to only those records where the number of tickets issued by a camera on a given date exceeds that camera's 99th percentile and dump the results:

        outliers = FILTER tickets_quantiles BY (ticket_count > quantile_99.($0));
        DUMP outliers;
        
  _**What's with the `$0` nonsense?**_ The output of our `Quantile` method is a tuple of quantiles rather than a scalar (this is so that it can simultaneously calculate multiple quantiles at once). If we simply performed our comparison as `(ticket_count > quantile_99)` Pig would complain that it cannot compare an `int` to a tuple. The `$0` notation references the first field (and in this case the only field) in the tuple; a scalar `int` value.

9. Quadruple-check your results. Your completed script should read:

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

10. Executing the script should produce output similar (but not necessarily identical) to:

        (1 E 63RD ST,2012-04-27,11,1 E 63RD ST,(10.0),4.145985401459854)
        (4400 W NORTH,2012-06-16,10,4400 W NORTH,(9.0),3.52463768115942)
        (4400 W NORTH,2012-09-29,11,4400 W NORTH,(9.0),3.52463768115942)
        (4400 W NORTH,2012-09-30,10,4400 W NORTH,(9.0),3.52463768115942)
        ...

  As we can see, the red light camera installed at State and 63rd Street had an usually good day on April 27th, 2012 having issued 11 tickets. On average, this camera issues 4.15 tickets per day and ten or fewer tickets on 99 percent of all days. 
  
  _**Why is my output different?**_ Your output may not start with the same four tuples shown above, but it should contain those records somewhere. This is a side effect of not ordering the `outliers` relation (like we did in the last exercise with the `ORDER ... BY` statement). As the Pig script is compiled into map-reduce jobs, Pig/Hadoop offers no guarantees regarding the order of the outputted tuples. 

**Extra credit:** The Chicago Tribune sometimes found that right before or right after a camera issued an abnormal number of tickets it issued very few (or zero) tickets making it seem as though these cameras were taken offline for reconfiguration. Try modifying this script to generate records where the ticket count is abnormally high or low. 

### Part 7: Count the appeal results by camera

Thus far our analysis has been concerned only with the quantity of tickets issues by each camera. Lets pull in the appeals data and determine how many appeals were filed and how tickets were overturned for each camera on each date. Like the last exercise, we'll again be making use of the CountEach UDF in the DataFu library, but this time to count both the number of tickets appealed and the number of tickets overturned during appeal. 

The algorithm we'll use to do so is:

* Load the hearing and tickets data
* Inner-join the two relations on the equality of ticket ID 
* Group the tickets and hearings by camera address and date
* Count the number of different result values (either `Liable` or `Not Liable`) for each group of tickets
* Output the camera address, date and the count of each result

_**Why not simply use the built-in `COUNT` function?**_ We could, but `CountEach` is more efficient as it enables us count multiple groups simultaneously. As noted [in the documentation](http://datafu.incubator.apache.org/docs/datafu/guide/bag-operations.html), without this UDF we'd have to perform two separate group operations resulting in two separate map-reduce jobs. 

1. Start by creating a new script and giving it a name like `ResultsByCamera`. Start the script by registering the DataFu library and aliasing the `CountEach` function:

        REGISTER datafu-1.2.0.jar
        DEFINE CountEach datafu.pig.bags.CountEach();

2. Load both the tickets and hearing data sets:

        hearings = LOAD 'default.admin_hearing_results_2012' USING org.apache.hive.hcatalog.pig.HCatLoader();
        tickets = LOAD 'default.all_rlc_tickets_2012' USING org.apache.hive.hcatalog.pig.HCatLoader();

3. Join the two tables on the basis of their ticket IDs being equal:

        joined = JOIN tickets BY ticket_id, hearings BY ticket_id; 

4. Group the joined results based on camera address and date, then count the number of liable and not-liable hearing results within each group:

        results = FOREACH (GROUP joined BY (camera_address, date)) GENERATE 
         	group as camera_by_date, CountEach(joined.(result)) as ticket_count;

5. Dump the results:

        DUMP results;

6. You completed script should read:

        REGISTER datafu-1.2.0.jar
        DEFINE CountEach datafu.pig.bags.CountEach();

        hearings = LOAD 'default.admin_hearing_results_2012' USING org.apache.hive.hcatalog.pig.HCatLoader();
        tickets = LOAD 'default.all_rlc_tickets_2012' USING org.apache.hive.hcatalog.pig.HCatLoader();

        joined = JOIN tickets BY ticket_id, hearings BY ticket_id; 

        results = FOREACH (GROUP joined BY (camera_address, date)) GENERATE 
         	group as camera_by_date, CountEach(joined.(result)) as ticket_count;

        DUMP results;

7. Provided the script executed successfully, you should see results similar (but not necessarily identical) to:

        ((1 E 63RD ST,2012-08-28),{((Liable),1)})
        ((1 E 63RD ST,2012-09-08),{((Not Liable),1)})
        ((1 E 63RD ST,2012-09-14),{((Liable),1)})
        ((1 E 63RD ST,2012-09-15),{((Not Liable),1),((Liable),2)})
        ...

  The meaning of results in this output may not be obvious. The outer tuple contains two elements: The first is a tuple containing the camera address and date (the elements we grouped in the `FOREACH ... GENERATE` statement. The second element is a bag containing a list of pairs in which each pair contains a hearing result (either `Liable` or `Not Liable`) followed by the number of tickets falling into each category. Tuples missing either result simply indicate that no tickets with that appeal disposition occurred on that date (for example, no tickets issued on August 28th by the camera at State and 63rd St were overturned during appeal). 
  
  You'll note that only tickets that were appealed are counted in this output; the sum of liable and not-liable tickets will not necessarily equal the total number of tickets issued by the camera on the given date.

### Part 8: Find cameras and dates that produced abnormal appeal success

In our final exercise, we'll attempt to identify camera and dates in which the courts threw out an abnormally large percentage of tickets. (Further implying that cameras may have been tampered with in such a way as to illegally ticket motorists).

Our process to calculate these results will be:

* Load the tickets and hearing data sets
* Join the tickets and hearing relations on the basis of ticket ID
* Filter the joined relation to remove all records not related to the camera at 6200 N. Lincoln Avenue (purely to reduce execution time)
* Count the number of liable and not-liable appeal results
* Calculate the average appeal success per date and order the results by this value

This will be largest and most complex script we'll write and it will have a commensurate execution time. This script may take as long as 30 minutes to run. Hard core "Porkers" (Pig aficionados?) will note that our process for calculating this data is not maximally efficient, but what it lacks in efficiency it makes up for in readability and understandability. 

Recall that in our last script we used DataFu's `CountEach` function; in this script, we'll see how we can count liable and not-liable tickets using only built-in functions.

1. Create a new script and give it a name such as `AppealSuccess`.
2. Begin by loading both the tickets and hearings datasets and joining the tables together:

        hearings = LOAD 'default.admin_hearing_results_2012' USING org.apache.hive.hcatalog.pig.HCatLoader();
        tickets = LOAD 'default.all_rlc_tickets_2012' USING org.apache.hive.hcatalog.pig.HCatLoader();
        joined = JOIN tickets BY ticket_id, hearings BY ticket_id; 

3. Purely for the sake of time, lets only consider the tickets issued by the camera at 6200 N. Lincoln Avenue (one of the cameras highlighted for abnormal behavior by the Chicago Tribune):

        liable = FILTER joined BY camera_address == '6200 N LINCOLN AVENUE' AND result == 'Liable';
        not_liable = FILTER joined BY camera_address == '6200 N LINCOLN AVENUE' AND result == 'Not Liable';

 Note that as part of the filtering process, we're also breaking out the data on the basis of appeal result. This operation will yield two relations, one containing all the ticket and appeal records where the motorist was found guilty on appeal, and the other containing records where the motorist was not (but in either case, only tickets issued by the Lincoln Avenue camera will be present in the output). 

4. At this point, we have a bag of ticket records. Let's group them by date and produce a relation containing only the fields we care about: the date of the ticket and the number of successful and failed appeals for that date. 

        liable = FOREACH (GROUP liable BY date) GENERATE
           group AS liable_issue_date,
           COUNT(liable) AS liable_cnt;

        not_liable = FOREACH (GROUP not_liable BY date) GENERATE
           group as not_liable_issue_date,
           COUNT(not_liable) as not_liable_cnt;

5. We could simply stop here and dump both tables (`liable` and `not_liable`) for a "raw" view of appeal success by camera and date, but let's cleanup the output a bit by calculating the appeal success rate and ordering the records on the basis of that field.

 We can't calculate an average of values stored in two different relations, so we'll first join the `liable` and `not_liable` relations.
 
         joined = JOIN liable BY liable_issue_date FULL OUTER, not_liable BY not_liable_issue_date;

  Note that we're resuing the `joined` alias for this output. Aliases are not "final" in Pig; this is a perfectly acceptable thing to do. 

6. Now that we've married the liable and not liable ticket counts, we can calculate the average appeal success rate for each date:

        results = FOREACH joined GENERATE
	       liable_issue_date,
           liable_cnt,
           not_liable_cnt,
           ((double)not_liable_cnt / (double)(liable_cnt + not_liable_cnt)) as appeal_success_rate;

  You'll note that we're _casting_ the counts to `double` before performing the arithmetic to produce the average (i.e., `(double)not_liable_cnt`). As is the case in many languages, without this step Pig would perform integer division yielding a 0/1 result. 

7. Lastly, we'll order the `results` relation by appeal rate success and dump the output. `DESC` indicates that the records should be ordered in descending order.

        ordered = ORDER results BY appeal_success_rate DESC;
        dump ordered;

8. Contestants playing along at home should have a finished script that reads:

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
        
9. Execute your script. Provided everything worked as expected, your script should have produced the following results:

        (2012-01-10,1,1,0.5)
        (2012-08-01,6,5,0.45454545454545453)
        (2012-01-04,5,4,0.4444444444444444)
        (2012-01-06,2,1,0.3333333333333333)
        ...