<div align="center">

# :zap: [Telegram-bot с прогнозом погоды](https://t.me/Weather_Forecast_44_Bot) :zap:

</div>

## :fire: В данном репозитории реализован проект :page_facing_up:, который включает в себя: 

 - ### [Приложение](https://github.com/badubidabambirimbum/weather-forecast-bot/tree/main/Parsing) :computer:, сделанное с помощью PyQt5, для сбора данных с сайтов: [Yandex](https://yandex.ru/pogoda?via=hl) и [GisMeteo](https://www.gismeteo.ru/). Позволяет собрать информацию о дневной температуре :sunny:, ночной температуре :crescent_moon: и прогнозе погоды :umbrella: на 10 дней вперед. В качестве примера используются 3 города: Москва, Краснодар и Екатеринбург.

 - #### Первый экран - сбор данных :cd:, backup :floppy_disk:, просмотр данных :mag:

<p align="center">
  <img src="photo/desktop_parsing_1.jpg" width="800" height="470">
</p>

 - #### Второй экран - добавление определенного дня :date:

<p align="center">
  <img src="photo/desktop_parsing_2.jpg" width="800" height="470">
</p>  

  - ### [jupyter файл](https://github.com/badubidabambirimbum/weather-forecast-bot/blob/main/create_dataset/analitic_temp_days.ipynb) :memo: с формированием базы данных, анализом зависимостей :chart_with_upwards_trend: и последующим отбором признаков :bar_chart: для модели нейронной сети:

<p align="center">
  <img src="photo/corr.png" width="800" height="470">
</p>

  - ### [telegram-bot](https://github.com/badubidabambirimbum/weather-forecast-bot/tree/main/telegram_bot) :robot: , который предоставляет прогноз погоды:

<p align="center">
  <img src="photo/qr-code.JPG" width="400" height="400">
</p>

  - ### :star: [Модель нейронной сети](https://github.com/badubidabambirimbum/weather-forecast-bot/tree/main/neural_network) для построения прогноза погоды на 10 дней вперед:

<p align="center">
  <img src="photo/model.jpg" width="800" height="470">
</p> 

  - #### Пример прогноза:

<p align="center">
  <img src="photo/predict_first_day.png" width="800" height="470">
</p> 
