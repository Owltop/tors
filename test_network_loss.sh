#!/bin/bash
# Тест потери пакетов

echo "=== Запуск системы ==="
docker compose up --build -d

echo "=== Имитация потери пакетов для worker3 (30%) ==="
sleep 3
docker ps
# docker compose exec worker3 tc qdisc add dev eth0 root netem loss 100%
docker compose exec -u root worker3 iptables -A OUTPUT -m statistic --mode random --probability 0.3 -j DROP # при 1 тоже работает

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