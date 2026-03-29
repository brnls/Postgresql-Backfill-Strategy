.\scripts.ps1 Help

.\scripts.ps1 Down

.\scripts.ps1 Up

.\scripts.ps1 Destroy

.\scripts.ps1 Seed

.\scripts.ps1 Seed -RowCount 1000000 -HotPct 2.5 -TemplateCount 128 -BlobTargetBytes 2500

.\scripts.ps1 RelationSize
.\scripts.ps1 ToastCheck

.\scripts.ps1 StartWorkload -UpdateClients 4 -UpdateThreads 4 -InsertClients 2 -InsertThreads 2 -WorkloadSeconds 600

.\scripts.ps1 BackfillGuid -BatchSize 1000 -LogEvery 100

.\scripts.ps1 StopWorkload

.\scripts.ps1 Results
.\scripts.ps1 Compare
asdfads