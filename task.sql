create procedure syn.usp_ImportFileCustomerSeasonal
	@ID_Record int
as
set nocount on  -- Ошибка 1: Ключевые слова SQL должны быть в верхнем регистре: SET NOCOUNT ON;
begin
	declare @RowCount int = (select count(*) from syn.SA_CustomerSeasonal)  -- Ошибка 2: Ключевые слова SQL — DECLARE, SELECT, FROM в верхнем регистре; Ошибка 3: Переменная объявлена, но не используется (лишняя)
	declare @ErrorMessage varchar(max)  -- Ошибка 4: Ключевые слова в верхнем регистре; объявление без точки с запятой

-- Проверка на корректность загрузки
	if not exists (  -- Ошибка 5: IF и EXISTS в верхнем регистре
	select 1  -- Ошибка 6: SELECT в верхнем регистре; также отсутствие отступа (неравномерный)
	from syn.ImportFile as f  -- Ошибка 7: FROM в верхнем регистре; алиас f не нужен, если не используется в запросе
	where f.ID = @ID_Record
		and f.FlagLoaded = cast(1 as bit)  -- Ошибка 8: CAST в верхнем регистре; лучше использовать 1 вместо CAST(1 AS BIT), так как FlagLoaded — битовое поле
	)
		begin  -- Ошибка 9: BEGIN в верхнем регистре и с правильным отступом
			set @ErrorMessage = 'Ошибка при загрузке файла, проверьте корректность данных'  -- Ошибка 10: SET в верхнем регистре; строка без точки с запятой

			raiserror(@ErrorMessage, 3, 1)  -- Ошибка 11: RAISERROR в верхнем регистре; отсутствует точка с запятой; желательно указывать уровень ошибки и состояние через параметры
			return  -- Ошибка 12: RETURN в верхнем регистре; лучше использовать RETURN 1; для явного указания кода ошибки
		end  -- Ошибка 13: END в верхнем регистре

	CREATE TABLE #ProcessedRows(ActionType varchar(255), ID int)  -- Ошибка 14: Конструкция CREATE TABLE без точки с запятой; рекомендуется проверять наличие таблицы перед созданием (DROP TABLE если существует); типы данных лучше писать в верхнем регистре VARCHAR(255), INT

	--Чтение из слоя временных данных
	select  -- Ошибка 15: SELECT в верхнем регистре
		cc.ID as ID_dbo_Customer  -- Ошибка 16: AS в верхнем регистре; стиль перечисления столбцов: запятые лучше ставить в конце строки, а не в начале
		,cst.ID as ID_CustomerSystemType
		,s.ID as ID_Season
		,cast(sa.DateBegin as date) as DateBegin  -- Ошибка 17: CAST и AS в верхнем регистре; также обращение к алиасу sa, который не объявлен, есть только cs
		,cast(sa.DateEnd as date) as DateEnd
		,cd.ID as ID_dbo_CustomerDistributor
		,cast(isnull(sa.FlagActive, 0) as bit) as FlagActive  -- Ошибка 18: ISNULL и CAST в верхнем регистре; избыточное приведение типов; также aliase sa не объявлен
	into #CustomerSeasonal  -- Ошибка 19: INTO в верхнем регистре; рекомендуется ставить после SELECT
	from syn.SA_CustomerSeasonal cs  -- Ошибка 20: JOIN-ы используют алиас sa, хотя таблица имеет алиас cs — ошибка в использовании алиасов
		join dbo.Customer as cc on cc.UID_DS = sa.UID_DS_Customer  -- Ошибка 21: Алиас sa не объявлен, должен быть cs
			and cc.ID_mapping_DataSource = 1
		join dbo.Season as s on s.Name = sa.Season  -- Ошибка 22: Аналогично, sa не объявлен
		join dbo.Customer as cd on cd.UID_DS = sa.UID_DS_CustomerDistributor
			and cd.ID_mapping_DataSource = 1
		join syn.CustomerSystemType as cst on sa.CustomerSystemType = cst.Name
	where try_cast(sa.DateBegin as date) is not null  -- Ошибка 23: TRY_CAST и AS в верхнем регистре; алиас sa не объявлен
		and try_cast(sa.DateEnd as date) is not null
		and try_cast(isnull(sa.FlagActive, 0) as bit) is not null  -- Ошибка 24: Избыточный каст; а также нельзя в WHERE проверять TRY_CAST на IS NOT NULL (неверная логика)

	-- Определяем некорректные записи
	-- Добавляем причину, по которой запись считается некорректной
	select  -- Ошибка 25: SELECT в верхнем регистре
		sa.*  -- Ошибка 26: Алиас sa не объявлен
		,case  -- Ошибка 27: CASE в верхнем регистре; запятая в начале строки — плохой стиль
			when cc.ID is null then 'UID клиента отсутствует в справочнике "Клиент"'  -- Ошибка 28: NULL в верхнем регистре
			when cd.ID is null then 'UID дистрибьютора отсутствует в справочнике "Клиент"'
			when s.ID is null then 'Сезон отсутствует в справочнике "Сезон"'
			when cst.ID is null then 'Тип клиента в справочнике "Тип клиента"'  -- Ошибка 29: Текст неинформативен — "Тип клиента в справочнике" звучит неполно, должно быть, например, "Тип клиента отсутствует"
			when try_cast(sa.DateBegin as date) is null then 'Невозможно определить Дату начала'  -- Ошибка 30: Повторяющаяся ошибка в условии ниже
			when try_cast(sa.DateEnd as date) is null then 'Невозможно определить Дату начала'  -- Ошибка 31: Текст ошибки дублирует предыдущий, должен быть "Невозможно определить Дату окончания"
			when try_cast(isnull(sa.FlagActive, 0) as bit) is null then 'Невозможно определить Активность'
		end as Reason
	into #BadInsertedRows  -- Ошибка 32: INTO в верхнем регистре; рекомендуется ставить после SELECT
	from syn.SA_CustomerSeasonal as cs  -- Ошибка 33: Опять используем cs, а выше sa — нужно единообразие
	left join dbo.Customer as cc on cc.UID_DS = sa.UID_DS_Customer  -- Ошибка 34: алиас sa не объявлен, нужно cs
		and cc.ID_mapping_DataSource = 1
	left join dbo.Customer as cd on cd.UID_DS = sa.UID_DS_CustomerDistributor and cd.ID_mapping_DataSource = 1  -- Аналогично
	left join dbo.Season as s on s.Name = sa.Season
	left join syn.CustomerSystemType as cst on cst.Name = sa.CustomerSystemType
	where cc.ID is null
		or cd.ID is null
		or s.ID is null
		or cst.ID is null
		or try_cast(sa.DateBegin as date) is null
		or try_cast(sa.DateEnd as date) is null
		or try_cast(isnull(sa.FlagActive, 0) as bit) is null


end  -- Ошибка 35: END в верхнем регистре; отсутствует точка с запятой
