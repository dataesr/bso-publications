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
	 - `doi` (Digital Object Identifier)
	 - `hal` (if present in HAL)
	 - `pmid` (if present in PubMed)

 -  **`externalIds.id`** Corresponding PID 


**`productionType`**  
String for type of production. Possible values: `publications`, `thesis`.


**`type`**  
String for more fine-grained type. Possible values: `journal-article`, `proceedings`, `book-chapter`, `book`, `preprint`, `thesis` and `other`. 


**`domains`**  
Array of objects

Lists detected scientific domains with

  - **`label.default`** Label of the scientific domain
  
  - **`code`** Code ID of this domain, if any (depends on `type`)
  
  - **`type`** Type of label. Possible values: `wikidata`, `keyword`, `sudoc`. Wikidata domains are automatically detected within the title, abstract and keywords. Keywords domain are exactly the free-text keywords that have been harvested (not normalized).
  
  - **`id_name`** Concatenation of code and label, separated by `##` 
  
  - **`count`** Number of times this code has been detected within the title, abstract and keywords.
  
  - **`naturalKey`** Normalized version of the label string (no accent, no space ...)

**`affiliations`**  
Array of objects

Lists all detected affiliations with

  - `id` PID of an affiliation. Can be the only field of the object if this affiliation is not mapped in the organization index.Otherwise, if present, some fields are denormalized:

  - `mainAddress`, `label`, `acronym`, `kind`, `level`, `status`, `isFrench`, `id_name`. See the documention of the organization index for more details on these fields.

**`isOa`**  
Boolean. Is Open Access ? 
Computed with Unpaywall data if DOI crossref, with HAL and theses.fr otherwise.

**`id`**  
String. Main PID of the production. This id is a concatenation of the id type (e.g `doi` and the id itself).

**`authorsCount`**  
Int. Number of authors.

**`landingPage`**
String. URL of the landing page.

**`urlPdf`**
String. URL of the PDF if any.

**`source`**
Object. Describe the source (generally journal) of the production, with

  - **`title`** Title of the source
  
  - **`publisher`** Publisher. Normalized version of the publisher.
  
  - **`journalIssns`** Arrays of ISSNs (print, electronic ...)
  
  - **`isOa`** Boolean. Is it an open access journal? (source Unpaywall)
  
  - **`isInDOAJ`** Boolean. Is it indexed in DOAJ? (source Unpaywall)

**`oaEvidence`**
Object. Describes the open access version (if any), with

  - **`hostType`** Possible values `publisher`, `repository`. Only one hostType is given, with a priority to the publisher, just like in the Unpaywall data. More complete data on open access routes can be found in the French Open Science Monitor data (Baromètre de la Science Ouverte).

  - **`version`** Open Access version (from Unpaywall data)
  
  - **`license`** Open Access version (from Unpaywall data or HAL)
  
  - **`url`** Open Access version (from Unpaywall data or HAL)
  
  - **`pdfUrl`** Open Access version (from Unpaywall data)

  - **`landingPageUrl`** Open Access version (from Unpaywall data)


**`keywords.default`**
List of string. Keywords (not normalized) chosen by the authors. These keywords are also present in the `domains` with type=`keyword`.


**`authors`**  


