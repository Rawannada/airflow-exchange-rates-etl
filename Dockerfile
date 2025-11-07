# Dockerfile (النسخة النهائية والمصححة)
FROM apache/airflow:2.9.2

# نستخدم المستخدم الافتراضي (airflow) لتثبيت حزم Python لتجنب أخطاء الصلاحيات
RUN pip install --no-cache-dir \
    # مزود MySQL (للاتصال بقاعدة بياناتك)
    apache-airflow-providers-mysql \
    # حزم ETL الرئيسية
    pandas \
    requests \
    # حزم مشروع الكتب
    beautifulsoup4 \
    matplotlib
    
# لا تحتاجين إلى سطر USER airflow هنا لأنها هي القيمة الافتراضية
# ولا تحتاجين إلى USER root إلا إذا كنتِ تثبتين حزم نظام التشغيل (apt-get)