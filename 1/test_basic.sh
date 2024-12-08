#!/bin/bash
# Базовый тест - проверка нормальной работы системы

echo "=== Запуск базового теста ==="
docker compose up --build -d

echo "=== Ожидание появления 'Итоговый результат' в логах ==="

# Функция для отслеживания появления строки в логах
function wait_for_result {
    while true; do
        # Проверяем логи на наличие строки "Итоговый результат"
        if docker compose logs | grep -q "Итоговый результат"; then
            echo "=== 'Итоговый результат' найден. Завершаем тест ==="
            break
        fi
        sleep 1 # Ожидание 1 секунду между проверками
    done
}

# Вызываем функцию ожидания
wait_for_result
docker compose logs

echo "=== Завершение теста ==="
docker compose down 
