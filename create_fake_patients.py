from concurrent.futures import ThreadPoolExecutor
from faker import Faker
import random

fake = Faker()
file = open("data.csv", "a")


def generate_patient_data(_):
    first_name = fake.first_name()
    last_name = fake.last_name()
    date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=80).strftime('%Y-%m-%d')
    gender = random.choice(['Male', 'Female'])
    address = fake.address().replace('\n', '').replace(',', '')
    phone_number = random.randint(6000000000, 9999999999)
    email = fake.email()
    diagnosis = random.choice(['Headache', 'Fever', 'Cough', 'Back Pain', 'Sprained Ankle', 'Sore Throat', 'Cold', 'Fatigue', 'Hypertension'])
    admission_date = fake.date_this_decade().strftime('%Y-%m-%d')
    discharged = random.choice([True, False])

    return f"{first_name}, {last_name}, {date_of_birth}, {gender}, {address}, {phone_number}, {email}, {diagnosis}, {admission_date}, {discharged}"

if __name__ == "__main__":
    num_records = 100000
    num_threads = 100

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Use list comprehension to generate 10,000 records using 10 threads
        insert_queries = list(executor.map(generate_patient_data, range(num_records)))

    # Print the generated insert queries
    for query in insert_queries:
        file.write(query + "\n")
