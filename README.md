# Introduction 
The purpose of the delivery is to facilitate an accounting platform to gather accounting data for KDA AS. Oracle ERP Cloud and Oracle APEX will be implemented to achieve this goal.

# Getting Started
TODO: Guide users through getting your code up and running on their own system. In this section you can talk about:
1.	Installation process
2.	Software dependencies
3.	Latest releases
4.	API references

# Build and Test
Build is done in APEX Application Development Service cloud environment accessed through browser. There is no local IDE needed for development.
All developers work in a shared environment, so it means our changes can impact everyone. Be mindful: a small change in your code can disrupt someone else’s work. Always communicate, document, and test carefully to avoid breaking the system for others.

Login to APEX development environment: https://g5c283cad42763c-gnlf29ztv3s1am8v.adb.eu-frankfurt-1.oraclecloudapps.com/ords/r/apex/workspace

# Project Structure
/Orafin
│
├── /db
│   ├── /tables               # SQL scripts for creating and managing tables
│   ├── /views                # SQL scripts for creating and managing views
│   ├── /packages             # PL/SQL package specifications and bodies
│   ├── /triggers             # SQL scripts for creating and managing triggers
│   ├── /sequences            # SQL scripts for creating and managing sequences
│   ├── /data                 # Scripts for inserting sample or lookup data
│   ├── /scripts              # Install/uninstall scripts for the database schema
│   └── README.md             # Documentation for the /db directory
│
├── /apex
│   ├── /app_export           # Exported APEX application files
│   └── README.md             # Documentation for the /apex directory
│
├── /erp                      # Oracle ERP custom objects
│   ├── /bi_catalog           # Reports, Data Models, OTBI reports, Dashboards, etc.
│   └── README.md             # Documentation for the /erp directory
│
├── /docs                     # Project documentation (architecture, design, etc.)
├── .gitignore                # Git ignore file for excluding unnecessary files
└── README.md                 # Project instructions and information

# Contribute
1.	Clone the Repository
Clone the repository to your local machine:
git clone https://CE003@dev.azure.com/CE003/Orafin/_git/Orafin

2.	Create a New Branch
Create a new branch for your feature or bug fix. Use a descriptive name related to your task:
git checkout -b feature/your-feature-name

3.	Make Your Changes
Make the necessary changes in your local branch. Ensure your code follows the project’s coding standards and guidelines.

4.	Commit Your Changes
Add and commit your changes with a meaningful commit message:
git add .
git commit -m "Add detailed description of what you have changed or fixed"

5.	Push Your Changes to the Remote Repository
Push your branch to the remote repository:
git push origin feature/your-feature-name

6.	Create a Pull Request
	•	Navigate to the repository on your Git platform (e.g., GitHub, GitLab).
	•	Open a Pull Request (PR) from your branch (feature/your-feature-name) into the develop branch.
	•	Provide a clear description of the changes, the problem they solve, and any additional context.

7.	Code Review and Approval
	•	The team will review your PR and provide feedback or approve it.
	•	Make any requested changes and update your PR.

8.	Merge the Pull Request
Once your PR is approved, it will be merged into the main branch. If you have permissions, you can do this yourself; otherwise, a project maintainer will merge it for you.

9.	Clean Up
	•	After your PR is merged, delete your feature branch from the remote repository and your local machine:
git branch -d feature/your-feature-name
git push origin --delete feature/your-feature-name


## Branches Used:

	•	main: Stable production-ready code.
	•	develop: Integrates all completed features; always in a deployable state.
	•	feature/*: Each feature has its own branch from develop.
    •	release/*: Prepares code for a new release, merges back to main and develop.
	•	hotfix/*: For urgent fixes on main; merges back to both main and develop.

## Workflow:

	1.	Start a Feature:
		Create a branch: feature/your-feature-name from develop.
		Implement and commit changes.
		Merge back to develop via a pull request (PR).
	2.	Create a Release:
		Create a branch: release/v1.0.0 from develop.
	    Perform final testing and bug fixing.
	    Merge to main and develop, and tag the release.
	3.	Hotfix a Problem:
	    Create a branch: hotfix/urgent-fix from main.
	    Implement the fix.
	    Merge to both main and develop.