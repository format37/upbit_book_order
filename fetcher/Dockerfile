FROM python:3.11
WORKDIR /server

COPY requirements.txt /server
RUN pip3 install -r requirements.txt --no-cache-dir
COPY symbols.txt /server
COPY fetch_order_book_upbit.py /server
CMD ["python", "fetch_order_book_upbit.py"]