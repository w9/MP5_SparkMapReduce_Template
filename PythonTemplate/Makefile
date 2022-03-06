.PHONY: partE
partE:
	(spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt partE && cat partE) 2>&1 | less -S
	
.PHONY: partD
partD:
	(spark-submit TopPopularLinksSpark.py dataset/links/ partD && cat partD) 2>&1 | less -S
	
.PHONY: partC
partC:
	(spark-submit OrphanPagesSpark.py dataset/links/ partC && cat partC) 2>&1 | less -S
	
.PHONY: partB
partB:
	spark-submit TopTitleStatisticsSpark.py partA partB
	
.PHONY: partA
partA:
	spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ partA

