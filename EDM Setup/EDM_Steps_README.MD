Here are the steps to setting u EDM classifiers in your Purview account:
Prequisits: You should aiready have set up a Sensitive Info Type. This SIT should match the EDM classifier primary column you are about to upload
1. In your Purveue portal, navigate to Information Protection > Classifiers >EDM classifiers
2. Create EDM classifier
3. Fill out Name, Description
4. Choose either Upload a file containing sample data (Recommended) or Manually define your data structure
5. Upload your csv file with data.
6. Select a Primary Element. Your purview would have already identified matching SIT
7. Complete setting such as proximity detection, Unique counting mode
8. Review and finish EDM creation

Now you should see a EDM classifier visible with "Schema not uploaded" under status. Now you should proceed to complete the upload as below to complete the creating of EDM.
Click on the EDM classifier and copy the EDMSchema name given there. You need this later for the file upload.

https://learn.microsoft.com/en-us/purview/sit-get-started-exact-data-match-hash-upload?tabs=single-computer
Assumption: you are using single computer mode
1. Use the above link and download EDM agent based on your requirement and complete the installation
2. Open command prompt/ Power sheel in Admin mode.
3. Switch dir to where you have installed your agent. (Recommended Dir is C:\EDM\Data)
4. Run this command to sign into your M365 account: EDMUploadAgent.ext/Authorize
5. Seperately, on your M365 Admin portal, create a security group named EDM_DataUploaders and give your account member privilage
6. Run this command to get the schema from your Purview account to local folder: EdmUploadAgent.exe /SaveSchema /DataStoreName [SchemaName] /OutputDir "C:\EDM\Data". This should save the xml schema file in given location
6. Back in your command prommp, run this command:

dmUploadAgent.exe /UploadData /DataStoreName [DS Name] /DataFile [data file] /HashLocation [hash file location] /Schema [Schema file] /AllowedBadLinesPercentage [value]

Eg: EdmUploadAgent.exe /UploadData /DataStoreName "surveyiddetectionedmschema" /DataFile "C:\Users\Banu\Desktop\SurveyID.csv" /HashLocation "C:\Users\Banu\EDMHash" /Schema "C:\EDM\Data\surveyiddetectionedmschema.xml" /AllowedBadLinesPercentage 5
For this code tu run successfully, 
- there should be a csv upload file at DataFile location
- Your schema name should match he one you got it from Schema Name you got from Purview portal earlier
- There should be a folder created at HashLocation to what you have used

Now, go back to Purview portaland make sure your EDM shows "Indexing 0%" under status. 
Wait for 24-48 hours to let your EMD classify your content from Exchange, SharePoint, OneDrive and Teams.