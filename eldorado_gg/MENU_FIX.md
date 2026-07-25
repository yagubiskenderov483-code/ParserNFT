# Eldorado GG — кнопки Отзывы + Миниапп

Репозиторий: `yagubiskenderov483-code/topOdinBot`  
Бот: `@EldoradoGGRobot`

## Что добавить в `main_kb`

Константы рядом с `SUPPORT_URL`:

```python
REVIEWS_URL  = "https://t.me/EldoradoProofs"
MINI_APP_URL = "https://www.eldorado.gg/"
```

Ряды меню:

```python
[Тех. поддержка] [Отзывы]      # url -> EldoradoProofs
[Миниапп]        [Наш сайт]    # Миниапп = WebApp, Наш сайт = обычная ссылка
[Как проходят сделки]
```

Полный файл: `eldorado_gg/bot.py` (или артефакт `eldorado_gg_bot.py`).

Залей в `topOdinBot` на `main` и перезапусти Eldorado GG.
