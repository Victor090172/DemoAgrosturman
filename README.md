# DemoAgrosturman
# eda-app

# Воспроизведение этого веб-приложения
Чтобы воссоздать это веб-приложение на своем компьютере, выполните следующие действия.

### Создайте среду conda
Во-первых, мы создадим среду conda под названием *eda*
```
conda create -n eda python=3.8
```
Во-вторых, мы войдем в среду *eda*
```
conda activate eda
```
### Установите необходимые библиотеки

Скачать requirements.txt файл

```
wget https://raw.githubusercontent.com/Victor090172/DemoAgrosturman/main/requirements.txt

```

Pip install libraries
```
pip install -r requirements.txt
```

###  Загружайте и распаковывайте содержимое из репозитория GitHub

Download and unzip contents from https://github.com/Victor090172/DemoAgrosturman/archive/main.zip

###  апустите приложение

```
streamlit run main.py
```
