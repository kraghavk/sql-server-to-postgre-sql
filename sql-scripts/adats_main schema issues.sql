-- adats_main schema issues

ALTER TABLE dbo.LandholdingLevels ADD CONSTRAINT
	PK_LandholdingLevels PRIMARY KEY CLUSTERED 
	(
	    ID
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

ALTER TABLE dbo.EUCShares DROP CONSTRAINT DF_EUCShareholders_IsReceiptPrinted;

ALTER TABLE dbo.EUCShares ADD CONSTRAINT DF_EUCShareholders_IsReceiptPrinted DEFAULT 0 FOR IsReceiptPrinted;
