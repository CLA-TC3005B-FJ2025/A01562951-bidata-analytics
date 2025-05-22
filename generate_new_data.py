import csv
import random
import faker

def generate_data(reg_qty, output_file):
    """
    Genera un archivo CSV con datos de ejemplo.
    """
    fake = faker.Faker()
    favorite_foods = ["Pizza", "Sushi", "Tacos", "Pasta", "Burgers", "Chocolate", "Ice Cream"]

    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["name", "nicname", "lucky_number", "country", "favorite_food"])

        for _ in range(reg_qty):
            name = fake.first_name()
            nickname = fake.user_name()
            lucky_number = random.randint(1, 100)
            country = fake.country()
            favorite_food = random.choice(favorite_foods)
            writer.writerow([name, nickname, lucky_number, country, favorite_food])

if __name__ == "__main__":
    register_amount = 100000
    output_file = 'data.csv'
    generate_data(register_amount, output_file)
    print(f"Archivo {output_file} generado con {register_amount} registros.")