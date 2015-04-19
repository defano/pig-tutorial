Chicago Coders Conference Big Data Hands on Lab
===============================================

In this exercise, we’ll be duplicating research performed by investigative reporters at the Chicago Tribune as part of their award-winning series on red light cameras in Chicago. Among other findings, the Tribune series discovered:

*	Select red-light cameras seemed to go on ticket-issuing benders; all of a sudden, affected cameras began nabbing drivers at a rate sometimes in excess of 50x their historical average.

*	Such spikes were often preceded and succeeded by periods of no activity, suggesting perhaps that cameras were reconfigured without mandated notice or documentation.

*	Drivers that appeal red-light tickets typically win their cases 10% of the time, but tickets issued during these spikes were overturned 45% of the time. 

Specifically, we’ll be reproducing the data (found in this article, http://apps.chicagotribune.com/news/local/red-light-camera-tickets/) illustrating periods of abnormally high ticketing activity and identifying any correlation to periods of appeal success. 

The full investigavtive series can be found here, http://www.chicagotribune.com/news/watchdog/redlight/. 

Getting Started
---------------

We’ll be using the Hortonworks Sandbox v2.2.4 as our “big data” platform during this exercise. The sandbox is freely available, installs trivially, and provides a user friendly interface (it even has its own built-in tutorial which you can use to improve your skillset outside this course). 

Most host machines should have little trouble running the sandbox, but official requirements from Hortonworks state:

*	Windows XP, Windows 7, Windows 8 or Mac OSX
*	Minimum 4GB RAM; 8GB required to run Ambari and Hbase
*	Virtualization enabled on BIOS
*	Browser: Chrome 25+, IE 9+, Safari 6+ recommended. (Sandbox will not run on IE 10)

1. Locate the VirtualBox software on the provided thumb drive and install it on your Mac or PC.

2. Import the Hortonworks virtual machine (OVA) into VirtualBox by double-click the `.ova` file on the thumb drive. Note that the import process may take several minutes. 

3. Once imported, click the "Start" button to run the virtual machine. The Hortonworks sandbox runs as a web app; after the machine has booted the virtual machine's console will display the URL to access the application. Open a browser and navigate to that URL: http://127.0.0.1:8888

4. Complete and submit the registration page and accept the terms of use.

5. The welcome page provides a link to the locally running application and displays the username and password to use for login. Navigate to [http://localhost:8000](http://localhost:8000) and, if prompted, log in with username `hue` and password `1111`.

Importing Data
--------------
