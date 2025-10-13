# Alumni Networking Tool

A web-based application designed to help the College of Engineering connect with students and alumni using the LinkedIn API. The tool provides an interface for exploring alumni profiles, networking opportunities, and fostering engagement between current students and graduates.

## Features

* **Alumni Search:** Find alumni by name, graduation year, degree, or department.
* **Profile Insights:** View LinkedIn profiles, career paths, and current positions.
* **Networking:** Connect students with alumni for mentorship, internships, and professional guidance.
* **Interactive Dashboard:** Visualize alumni distribution by location, industry, and role.
* **Secure LinkedIn Integration:** Uses LinkedIn API for secure login and access

## Getting Started

### Prerequisites

* Python 3.10+
* LinkedIn Account

### Installation

1. Clone the repository:

```bash
git clone https://github.com/sangambartaula/alumni-networking-tool
cd alumni-networking-tool
```

2. Create a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate  # macOS/Linux
venv\Scripts\activate     # Windows
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Set up environment variables (LinkedIn credentials, etc.) in a `.env` file.

5. Run the application:

```bash
python app.py
```

Open your browser at `http://localhost:5000`

## Running Tests

### Backend Tests (Python)

Run all backend tests with pytest:

```bash
pytest backend/tests/ -v
```

Run with coverage report:

```bash
pytest backend/tests/ --cov=backend --cov-report=html
```

### Frontend Tests (JavaScript)

Install Node.js dependencies first (one-time setup):

```bash
npm install
```

Run all frontend tests with Jest:

```bash
npm test
```

Run with coverage report:

```bash
npm run test:coverage
```

## Project Structure

```
alumni-networking-tool/
│
├── backend/               # Backend Python code
│   ├── app.py            # Flask application
│   ├── database.py       # Database utilities
│   └── tests/            # Backend tests
├── frontend/              # Frontend code
│   ├── public/           # HTML templates and static assets
│   │   ├── assets/       # Images, icons, other media
│   │   ├── app.js        # Frontend JavaScript
│   │   └── index.html    # Main HTML file
│   └── tests/            # Frontend tests
├── requirements.txt       # Python dependencies
├── package.json          # Node.js dependencies and scripts
├── README.md             # Project documentation
└── .env                  # Environment variables (not committed)
```

## Group Members

* **Sangam Bartaula** - @sangambartaula
* **Sachin Banjade** - @sbanjade
* **Abishek Lamichhane** - @lamichhaneabishek1
* **Shrish Acharya** - @Shrish63
* **Niranjan Paudel** - @aashishs421
