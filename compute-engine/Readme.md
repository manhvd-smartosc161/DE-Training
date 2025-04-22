# Truy cập vào cloud

gcloud compute ssh \
 --zone "us-central1-c" \
 "my-vm-instance" \
 --project "training-de-457603"

# Fix lỗi không truy cập IP của container

# Kiểm tra xem firewall rule đã tồn tại chưa

gcloud compute firewall-rules list | grep "allow-http"

# Nếu chưa có, tạo firewall rule cho port 80

gcloud compute firewall-rules create allow-http \
 --direction=INGRESS \
 --priority=1000 \
 --network=default \
 --action=ALLOW \
 --rules=tcp:80 \
 --source-ranges=0.0.0.0/0 \
 --target-tags=http-server

# Thêm tag http-server cho VM

gcloud compute instances add-tags my-vm-instance \
 --zone=us-central1-c \
 --tags=http-server

# Result

http://34.71.203.29/
