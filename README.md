# Million Songs analysis
This project is a comprehensive analysis of song data spanning several decades. It aims to uncover insights and trends in the music industry over time. The data analyzed includes various parameters such as the artist’s name, the year of song release, the singer’s gender, and the country of origin.


![flowchart](https://github.com/MhmedRjb/MillionSonganalysis/assets/72052305/af65906c-6741-486d-a890-83956b85f160)

## Tech Stack & Tools

- **Infrastructure**: Terraform & Docker
- **Orchestration**: MAGEai
- **Database Storage**:local
- **Data Processing**: Apache Spark
- **ETL Scripts**: Python
- **Serving Layer**: Google Sheets & Looker

## Pipeline Overview

The pipeline starts by ingesting raw data from CSV files. and collect another data using wikidata API Following the ETL (Extract, Transform, Load) process, The orchestration of the ETL workflow is  MAGEai, then it save to googlesheet file in cloud. Finally, the insights derived from the processed data are visualized using lookerstudio.


## Getting Started

This section will guide you through getting the project up and running on your local machine for development and testing purposes.

### Prerequisites

- Docker
- Docker Compose
- Terraform

### Installation & Setup

1. Download the Repository
2. Open Google Cloud  
  1. Create a Service Account in the project
  2. Generate a key and save it in the project path as **Serviceaccounts.json**
3. Replace the existing file with this new key
4. Copy this [google sheet](https://docs.google.com/spreadsheets/d/15AodsCwcQNIwR4msZSvm47uH4O-Q-8HEoQq7mYy_KUc/edit#gid=357445582) to your account with the same name
  5. copy the client_email from **Serviceaccounts.json** file and make it editor in google sheet by click 
6. Open Looker Studio and copy this [report](https://lookerstudio.google.com/reporting/fa458839-85d2-4ac6-a5fd-94acfbcbd2ae/page/cj5uD/edit)
7. Define the data sources which is the Google Sheet file
8. Run this command to build the infrastructure:
``` terraform apply ```
Select 'yes' when prompted
Run this command to trigger the pipeline:
``` curl -X POST http://localhost:6789/api/pipeline_schedules/1/pipeline_runs/5266e37a5e6545bb8d96531bf70471d5 ```
If the pipeline doesn't start automatically, navigate to server: localhost:6789 and click on MillionSongsanalysis, then select 'run once'

in case you get a model not found error go to requirements.txt and install packages 

After completing the above steps, the setup should be functional
### Visualizations
[lookerstudio report](https://lookerstudio.google.com/s/iPMKjuBqdHQ))
![singers_data_visualization_page-0001](https://github.com/MhmedRjb/MillionSonganalysis/assets/72052305/af26fac9-34c6-4c31-b3f0-8f68338d628a)
