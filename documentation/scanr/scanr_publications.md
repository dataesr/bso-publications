**`summary.default`** , **`summary.en`**, **`summary.fr`**  
String

Abstract or summary of the publication 

`.default` for preferred language (or unknown language), `.en` for english, `.fr` for french`


**`publicationDate`**  
String for publication date

Formatted as "YYYY-MM-DDT00:00:00",

When available, the `published_date` from Unpaywall is used.

When not avaiable (no DOI Crossref), if data comes from HAL, these fields from HAL are used, in this priority order: `publicationDate_s`, `ePublicationDate_s`, `defenseDate_s`, `producedDate_s`.


**`year`**  
Int for year of publication

Year part of the publication Date


**`externalIds`**  
Array of objects

Lists the known external identifiers with 

 - **`externalIds.type`**  Possible values are:
	 - `nnt` (for French thesis, `nnt`stands for `Numéro National de Thèse`)
	 - `doi`(Digital object

 -  **`externalIds.id`**  


**`productionType`**  


**`type`**  


**`domains`**  


**`affiliations`**  


**`isOa`**  


**`id`**  


**`authorsCount`**  


**`authors`**  


