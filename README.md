# Data Engineering Exercise

## Objective:
Design a simplified version of a data pipeline using a real-world scenario. The exercise focuses on your ability to integrate data from an external API, process it, and present insights in a manner that's valuable for product and analytics teams.

## Scenario:
 You are a part of a small online book company and your company wants to expand on the books provided by their online store. Your task if you choose to accept it, is to develop a mini data pipeline that extracts data from [Open Library's API](https://openlibrary.org/developers/api), focusing on a [subject](https://openlibrary.org/dev/docs/api/subjects) of your choice.

This pipeline will be generalized for production use at a later date as the company wishes to perform similar analysis in the future. The data to be extracted includes a list of authors and books related to your chosen subject.
import requests
url = "https://openlibrary.org/search.json?q=test"
headers = {
    "User-Agent": "MyAppName/1.0 (myemail@example.com)"
}
response = requests.get(url, headers=headers)
## Key Deliverables
1. **Architecture Overview**:
   - Briefly outline the architecture and the technologies or services you would use. As we use AWS, mention any relevant AWS services.
   - Include a simple data model diagram.


3. **Sample Data Output**:
   - Provide CSV outputs for 'Authors' and 'Books'.
   - Include a simple 'Authors and Books' relation (no need for a full bridge table).

4. **Data Processing**:
   - Write a SQL query (or Python equivalent) to aggregate:
     - The number of books written each year by an author.
     - The average number of books written by an author per year.
   - Discuss how you would optimize this for a larger dataset.

5. **Presentation**:
   - Prepare a short presentation explaining your approach, decisions made, and any assumptions.
   - Discuss what you would do differently in a production environment and potential enhancements.

6. **ReadMe & Documentation**:
   - Include a brief ReadMe file explaining your approach.
   - Document any setup instructions or prerequisites.

## Duration:
- This exercise is designed to be completed within a few hours. Focus on working code and explaining that code to the interviewers
- Focus on demonstrating your thought process, ability to work with data, and how you would communicate your findings to product and analytics teams.

**Evaluation Criteria**:
- Clarity of thought and communication.
- Practicality and relevance of the data pipeline design.
- Coding style and data handling in Python.
- Understanding of basic data aggregation and SQL queries.
- Ability to think about scalability and production-readiness.

**Optional Enhancements** (if time permits):
- Include basic error handling or data validation in your script.
- Discuss security considerations, especially in the context of AWS.
- Briefly mention how you would incorporate infrastructure as code, like Terraform.

**Note**:
- It's okay to leave out certain aspects for time's sake. Prioritize the core components and be ready to discuss what you would add or change for a real-world scenario.
- Your presentation and discussion will be a key part of the interview, allowing us to understand your approach and problem-solving skills.

## Additional Info:
* we expect that this will take you a few hours to complete
* use any language or framework you are comfortable with, we prefer Python
* AWS is our cloud provider please include mention of the services you would choose
* Bonus points for including terraform or any other infrastructure as code
* Bonus points for security, specs, tests etc.
* Do as little or as much as you like.

Please fork this repo and commit your code into that fork. Show your work and process through those commits.
