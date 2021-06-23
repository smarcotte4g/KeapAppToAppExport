# Why?

This tool was created for Keap Data Specialist to export applications. The format exported is created in a way to make it easy to import from the front end. If the tag id question is left blank, all the data will be exported. However if a tag id is added, only the contacts and those contacts data will be exported. For example, only products that have been purchased by the contacts with that tag specified will be exported.

# Instructions

git clone https://github.com/smarcotte4g/KeapAppToAppExport.git

pip install any dependencies missing check requirements.txt

Login to PuTTY or Host

Use 'PortForwardApps APPNAME' to get port

python main.py

Enter App Name

Enter Port

Enter 1 tag id if you are exporting a subset of tags

Output will tell you how many of each section and how many exported

Export is added to /app/{appname}
