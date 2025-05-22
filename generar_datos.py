import csv
import random
import faker

def generar_datos(num_registros, archivo_salida):
    """
    Genera un archivo CSV con datos de ejemplo.
    """
    fake = faker.Faker()

    with open(archivo_salida, mode='w', newline='') as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["nombre", "edad", "ciudad"])

        for _ in range(num_registros):
            nombre = fake.first_name()
            edad = random.randint(18, 80)
            ciudad = fake.city()
            writer.writerow([nombre, edad, ciudad])

if __name__ == "__main__":
    numero_registros = 10000
    archivo_salida = 'data.csv'
    generar_datos(numero_registros, archivo_salida)
    print(f"Archivo {archivo_salida} generado con {numero_registros} registros.")