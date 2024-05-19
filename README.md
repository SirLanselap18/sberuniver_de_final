<h1>Обзор</h1>
<p>Проект <strong>sberuniver_de_final</strong> – это учебный ETL (Extract, Transform, Load) процесс, разработанный для автоматизации загрузки, трансформации и анализа финансовых данных с целью создания отчетов о мошеннических операциях.</p>

<h2>Основные Компоненты</h2>
<h3>Выгрузка данных</h3>
<p>Ежедневная выгрузка данных включает:</p>
<ul>
  <li>Список транзакций за текущий день (CSV)</li>
  <li>Список терминалов (XLSX)</li>
  <li>Список паспортов в “черном списке” (XLSX)</li>
</ul>

<h3>Хранилище данных</h3>
<p>Данные загружаются в учебную базу данных (edu) с определенной структурой, включая технические поля для отслеживания изменений (SCD1 и SCD2).</p>
<img src=https://github.com/SirLanselap18/sberuniver_de_final/blob/main/static/db.png?raw=true alt="схема данных">

<h3>Построение отчета</h3>
<p>Ежедневное создание витрины отчетности по мошенническим операциям с данными о времени события, номере паспорта, ФИО, телефоне клиента и типе мошенничества.</p>
<table>
  <tr>
    <th>event_dt</th>
    <th>passport</th>
    <th>fio</th>
    <th>phone</th>
    <th>event_type</th>
    <th>report_dt</th>
  </tr>
  <tr>
    <td>Время наступления события. Если событие наступило по результату нескольких действий – указывается время действия, по которому установлен факт мошенничества.</td>
    <td>Номер паспорта клиента, совершившего мошенническую операцию.</td>
    <td>ФИО клиента, совершившего мошенническую операцию.</td>
    <td>Номер телефона клиента, совершившего мошенническую операцию.</td>
    <td>Описание типа мошенничества (номер).</td>
    <td>Дата, на которую построен отчет.</td>
  </tr>
</table>

<h3>Признаки мошеннических операций</h3>
<p>Определены четыре основных признака для идентификации мошеннических операций:</p>
<ol>
  <li>Совершение операции при просроченном или заблокированном паспорте.</li>
  <li>Совершение операции при недействующем договоре.</li>
  <li>Совершение операций в разных городах в течение одного часа.</li>
  <li>Попытка подбора суммы. В течение 20 минут проходит более 3х операций со следующим шаблоном – каждая последующая меньше предыдущей, при этом отклонены все кроме последней. Последняя операция (успешная) в такой цепочке считается мошеннической.</li>
</ol>

<h3>Обработка файлов</h3>
<p>Выгружаемые файлы именуются согласно следующему шаблону:</p>
<pre>
transactions_DDMMYYYY.txt
passport_blacklist_DDMMYYYY.xlsx
terminals_DDMMYYYY.xlsx
</pre>
<p>Предполагается, что в один день приходит по одному такому файлу. После загрузки соответствующего файла он должен быть переименован в файл с расширением .backup чтобы при следующем запуске файл не искался и перемещен в каталог archive:</p>
<pre>
transactions_DDMMYYYY.txt.backup
passport_blacklist_DDMMYYYY.xlsx.backup
terminals_DDMMYYYY.xlsx.backup
</pre>

<h3>Основной рабочий код</h3>
<p>Расположен в файле main.py, содержит логику подключения к базам данных, функции для извлечения и трансформации данных, а также процедуры для загрузки данных в хранилище и построения отчетов.</p>
<p>Файл mail.ddl содержит в себе запросы для создания таблиц.</p>
<p>Файл main.cron.txt демонстрирует создание ежедневного задания в планировщике.</p>

<h3>Использованные библиотеки и модули</h3>
<ul>
  <li><code>psycopg2</code>: Библиотека для работы с PostgreSQL. Используется для подключения к базе данных и выполнения SQL-запросов;</li>
  <li><code>pandas</code>: в данном проекте данная библиотека в основном применяется для обработки поступающих извне xlsx и csv файлов и последующего переноса данных в таблицы PostgreSQL;</li>
  <li><code>os</code>: Встроенный модуль Python для взаимодействия с операционной системой. В данном проекте применялся для работы с поступающими файлами: переноса обработанных файлов в папку archive, проверки наличия файлов для обработки, наполнения лога;</li>
  <li><code>datetime</code>: Модуль для работы с датами и временем. Используется для обработки и преобразования дат в нужный формат.</li>
</ul>

<h1>Заключение</h1>
<p>Проект является комплексным решением для обучения и практики в области обработки данных и разработки ETL процессов, с акцентом на реальные задачи, связанные с финансовыми операциями и их анализом. По итогам поставленных задач были обнаружены все мошеннические операции, которые предполагались заданием.</p>