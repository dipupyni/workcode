#@title Текст заголовка по умолчанию
"""Игра угадай число
Компьютер сам загадывает и сам угадывает число
"""

import numpy as np
import statistics

def random_predict(number:int=np.random.randint(1, 101)) -> int:
    """Рандомно угадываем число

    Args:
        number (int, optional): Загаданное число. По умолчанию рандомно загадывается компьютером в диапазоне 1-100.

    Returns:
        int: Число попыток 
    """
    count = 0
    lst_num = list(range(1, 101))

    while True:
      count += 1
      predict_number = int(np.mean(lst_num))
      half = round(int(len(lst_num))/2)
      if number == predict_number:
        break
      elif predict_number < number:
        lst_num = lst_num[half:]  
      else:
        lst_num = lst_num[:half]
      if len(lst_num) == 0:
        break

    return count

def score_game(random_predict) -> int:
    """За какое количство попыток в среднем за 1000 подходов угадывает наш алгоритм

    Args:
        random_predict ([type]): функция угадывания

    Returns:
        int: среднее количество попыток
    """
    count_ls = []
    #np.random.seed(1)  # фиксируем сид для воспроизводимости
    random_array = np.random.randint(1, 101, size=(1000))  # загадали список чисел

    for number in random_array:
        count_ls.append(random_predict(number))

    score = int(np.mean(count_ls))
    print(f"Ваш алгоритм угадывает число в среднем за:{score} попыток")
    return score


if __name__ == "__main__":
    # RUN
    score_game(random_predict)