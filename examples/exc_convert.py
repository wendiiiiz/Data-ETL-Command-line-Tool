import sys
import csv


DIGIT_MAPPING = {'zero': '0', 'one': '1', 'two': '2', 'three': '3', 'four': '4', 'five': '5',
                 'six': '6', 'seven': '7', 'eight': '8', 'nine': '9', 'ten': '10'}


def convert(input_param):                         # Define function
    converted = ''
    for element in input_param:
        converted += DIGIT_MAPPING[element]

    converted_target = int(converted)       # Convert string to int
    return converted_target                 # Return converted int


def convert_with_keyerror(input_param):     # Define function
    converted = ''
    try:                                    # try-block
        for element in input_param:
            converted += DIGIT_MAPPING[element]

        converted_target = int(converted)   # Convert string to int
        print(f"Conversion succeeded: {converted_target}")  # Print statement to help understand normal workflow
    except KeyError:                        # except-block
        converted_target = -1
        print(f"Conversion failed: {converted_target}")     # Print statement to help understand exceptional workflow
    return converted_target                 # Return converted int


def convert_with_exc(input_param):     # Define function
    converted = ''
    converted_target = -1
    try:                                    # try-block
        for element in input_param:
            converted += DIGIT_MAPPING[element]

        converted_target = int(converted)   # Convert string to int
    except (KeyError, TypeError):           # except-block
        pass
    return converted_target                 # Return converted int


def convert_with_exc_access(input_param):   # Define function
    converted = ''
    converted_target = -1
    try:                                    # try-block
        for element in input_param:
            converted += DIGIT_MAPPING[element]
    except (KeyError, TypeError) as exc:    # except-block
        print(f"Exception was caught: {exc}", file=sys.stderr)
        return -1

    return int(converted)                   # Convert string to int


def lookup():
    input_list = [1, 2, 3]
    try:
        element = input_list[5]
    except IndexError:
        print("Handled IndexError")

    input_dict = {1: 'value1', 2: 'value2', 3: 'value3'}
    try:
        element = input_dict[5]
    except KeyError:
        print("Handled KeyError")


def lookup_enhanced():
    try:
        input_list = [1, 2, 3]
        element = input_list[5]

        input_dict = {1: 'value1', 2: 'value2', 3: 'value3'}
        element = input_dict[5]
    except LookupError as exc:
        print(f"Handled LookupError: {exc.args}")


def read():
    file_handler = None

    try:
        file_handler = open("c:\temp\sample.csv", 'rb')
        reader = csv.reader(file_handler)
        for row in reader:
            print(row)
    except FileNotFoundError as fileError:
        print("File does not exist: c:\temp\sample.csv")
    except Exception as exc:
        print(f"Exception raised: {exc}")
    else:
        print(f"Reader: {type(reader)}")
    finally:
        if file_handler:
            file_handler.close()


if __name__ == '__main__':
    # Call main process.
    read()
