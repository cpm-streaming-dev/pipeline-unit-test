# pipeline-unit-test

# COMMAND 
# สร้างโฟลเดอร์ venv สำหรับเก็บ virtual environment
python -m venv venv

# เปิดใช้งาน virtual environment
source venv/Scripts/activate

pip install -r requirements.txt

pipeline-unit-test/
├── .gitignore
├── README.md
├── requirements.txt
│
├── config/                 # <-- โฟลเดอร์ใหม่สำหรับเก็บ Config
│   ├── test_config.ini
│   ├── endpoints.ini
│   └── kafka_config.ini
│
├── src/                    # โฟลเดอร์เก็บ Logic
│   ├── __init__.py
│   ├── sql_builder.py
│   ├── sql_executor.py
│   ├── kafka_consumer.py
│   └── asserter.py
│   └── test_selector.py
│
├── run_e2e_tests.py         # สคริปต์หลักสำหรับควบคุม
│
└── tests/
    ├── inputs/
    └── expected/