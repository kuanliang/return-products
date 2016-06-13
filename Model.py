def learn_model(X, y, colInfo):
    """

    """
    print 'modeling start...'
    #print 'X: {}'.format(type(X))
    #preprocessing = FeatureUnion([("initial", InitialProcessor())])
    IP = InitialProcessor(colInfo=colInfo.value)

    # print 'X value: {}'.format(type(X))
    X = pd.DataFrame(X.value, columns=colInfo.value['preprocess']['all'])
    X = IP.transform(X)
    pipe = Pipeline([
            #('union', InitialProcessor(colInfo=colInfo)),
            ('impute', Imputer(missing_values = 'NaN', strategy = 'median', axis = 1)),
            ('scaler', RobustScaler()),
            #('clf', linear_model.SGDClassifier(loss = 'log', penalty='l1', class_weight='balanced', n_iter=1))
            ('clf', linear_model.LogisticRegression(class_weight='balanced'))
        ])

    param_grid = {
        'clf__penalty': ('l1', 'l2'),
        'clf__tol': (1, 1e-1, 1e-2, 1e-3, 1e-4),
        'clf__C': (10, 5, 1, 0.1, 0.01, 0.001, 0.0001)
    }
    X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size = 0.3, random_state=0)
    gs_cv = grid_search.GridSearchCV(pipe, param_grid, scoring='precision')
    gs_cv.fit(X_train, y_train)
    predict_test = gs_cv.predict(X_test)
    predict_train = gs_cv.predict(X_train)
    prediction_report_test = classification_report(y_test, predict_test)
    prediction_report_train = classification_report(y_train, predict_train)
    # self.predicion_report_test = prediction_report_test
    # self.prediction_report_train = prediction_report_train
    return gs_cv, prediction_report_test, prediction_report_train

