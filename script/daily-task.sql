REQUIRED:
+ Rerun TLBBM (ETL, SERVER, GAME)
+ RERUN STONYVN (ETL, SERVER, GAME)
+ RERUN CONTRA SERVER from start to 01/09
+ RERUN 3QMOBILE ETL
+ RERUN CTPGSN from open date
+ RERUN Sky Garden Global from open date
+ RERUN Tini Farm from open date
----------------------------------------------------------------------------------------------------------------

RUNNING:
+ TLBBM:
	
	* ETL: 
		open -> now: OK
	* GAME: 
		open -> now: OK
	* SERVER:

+ Nikki:
	* ETL:
		open -> now: OK
	* GAME:
		open -> now: OK
	* SERVER:

+ CONTRA:
	* ETL:
		open -> now: OK
	* GAME:
		open -> now: OK
	* SERVER:
		01/09 -> now: OK

+ 3QMOBILE:
	* ETL:
		open -> now: OK
	* GAME:
	* SERVER:

+ Stonyvn:
	* ETL:
		open -> now: OK
	* GAME:
		open -> now: OK
	* SERVER:

+ CTPGSN: waiting log from open
	* ETL:
		open -> 31/10: OK
		run daily
	* GAME:
	* SERVER:

+ Sky Garden Global:
	* ETL:
		open -> now: OK
		run daily
	* GAME:
		open -> now: OK
	* SERVER:	no server report (sid = 0)
+ Cube Farm Global: verifing log info
	* ETL: run ETL daily
	* GAME:
		01/09 -> 31/10: OK
	* SERVER:

+ Tini Farm: waiting log from open
	* ETL:
		01/01 -> 22/07: OK
		22/07 -> 31/10: OK
		run daily
	* GAME:
		01/09 -> 31/10: OK
	* SERVER:
+ PMCL:
	* ETL:
		18/03 -> 15/10: OK
		01/09 -> 31/10: OK
	* GAME:
		01/10->31/10: OK:
		RUN DAILY
	* SERVER:
+ KFTL:
	* ETL:
		open -> 01/08: OK
		01/08 -> 31/10: OK
	* GAME:
		01/10 -> 15/11: running
	* SERVER:
+ DPTK:
	* ETL:
		open -> 31/10: OK
		run daily
	* GAME:
		01/09 -> 31/10: OK
	* SERVER:
+ Game Name:
	* ETL:
	* GAME:
	* SERVER:

----------------------------------------------------------------------------------------------------------------

DONE:
oozie job -suspend 0071412-161018005711488-oozie-oozi-B
oozie job -suspend 0070198-161018005711488-oozie-oozi-B
----------------------------------------------------------------------------------------------------------------