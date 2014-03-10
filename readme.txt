1. Run wget:
wget.exe -N -o download_edgar_index.log -r --accept="form.*" ftp://ftp.sec.gov/edgar/daily-index/
2. Repeat until no new files are downloaded.
3. Unzip all .gz files and delete the original gz files.
4. Run  NSAR_collect.py to create the DB of all EDGAR filings, and to create NSARXXX.txt, for all NSAR types.
5. Copy NSARXXX.txt to appropriate folder
6. Run wget for each file:
C:\DATA\Kaniel\NSAR\wget\NSARB>..\wget -N --retr-symlinks -o download_NSARB.log -i NSARB.txt
