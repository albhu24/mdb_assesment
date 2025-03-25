# Time-Series Event Microservice

This is a backend microservice built with Python and FastAPI that emits time-series events at regular intervals and provides a history of past events. 

## Prerequisites
- Python 3.11+

## Setup and Installation

1. **Clone the Repository**
   ```bash
   git clone https://github.com/albhu24/mindsDB_assesment.git
   cd src
   pip install -r requirements.txt
   uvicorn src.main:app

## Future Improvements
- Add a query parameter to /stream for customizable intervals (e.g., ?interval=5).
- Implement unit tests with pytest.
- Add logging and monitoring.
- Include input validation and more robust error handling.
