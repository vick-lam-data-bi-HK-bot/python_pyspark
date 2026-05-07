# python_pyspark

## PySpark MCQ Revision Website

This repository contains a static, interactive multiple-choice quiz for reviewing end-to-end Spark/PySpark pipeline concepts. It is designed for students who want to practice architecture, ingestion, transformation, sink, optimization, and testing topics with model answers and explanations.

### What is included

- `index.html` — the quiz user interface
- `styles.css` — styling for the page and quiz cards
- `script.js` — question data, answer selection, and feedback logic

### Topics covered

1. Architecture & Core Concepts
2. Data Ingestion & Source Control
3. Transformation Logic & Performance
4. Data Sink & Output Management
5. Optimization & Debugging
6. Testing, Security & CI/CD
7. Interview Short Questions for enterprise banking pipeline practice

### How to use locally

1. Open `index.html` directly in your browser to start the quiz.
2. Select a section filter or choose `All Sections`.
3. Choose an answer for each question.
4. Click **Check Answer** to validate your choice.
5. Click **Show Model Answer** to reveal the correct answer and a short explanation.

> Tip: For the best local experience, serve the site from a local web server instead of opening the file directly.

### Local web server (recommended)

From the repository root, run:

```bash
python3 -m http.server 8000
```

Then open:

```text
http://localhost:8000
```

### Deploying the site

You can deploy this static site to any static hosting service.

#### GitHub Pages

1. Commit and push the repository to GitHub.
2. Open the repository settings in GitHub.
3. Go to **Pages**.
4. Set the source branch to `main` and folder to `/ (root)`.
5. Save and wait for the published URL.

#### Other static hosts

- Netlify
- Vercel
- Surge
- Firebase Hosting

Simply point the host to this repository folder and deploy.

### Customization ideas

- Add more questions or update existing explanations.
- Add a scoring summary for correct/wrong answers.
- Add search or bookmark support for review sessions.
- Convert to a React or Vue app for more advanced interactivity.

### Notes

This static website does not require a server-side backend. It is lightweight and suitable for quick Spark/PySpark revision.
