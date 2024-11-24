#!/bin/bash
# Тест отказа воркера

echo "=== Запуск системы ==="
docker compose up --build -d

echo "=== Имитация падения worker2 ==="
docker compose stop worker2

echo "=== Ожидание 10 секунд ==="
sleep 10

echo "=== Перезапуск worker2 ==="
docker compose start worker2

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
