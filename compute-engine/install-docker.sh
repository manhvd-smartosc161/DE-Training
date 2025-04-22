# Cập nhật package index
sudo apt update

# Cài các gói phụ thuộc
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common

# Thêm Docker GPG key
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

# Thêm Docker repository
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

# Cập nhật lại package và cài Docker
sudo apt update
sudo apt install -y docker.io

# Kiểm tra Docker đã cài đặt thành công
sudo docker --version

# Thêm user hiện tại vào Docker group (không cần sudo)
sudo usermod -aG docker $USER
