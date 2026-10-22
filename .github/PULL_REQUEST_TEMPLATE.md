## Thay đổi gì

<!-- Một PR một việc. Mô tả ngắn: sửa gì, ở video/thư mục nào, vì sao. -->

## Loại thay đổi

- [ ] Sửa lỗi trong lab / code
- [ ] Sửa chính tả, câu chữ trong README / docs
- [ ] Thêm bài tập / nội dung mới
- [ ] Thay đổi môi trường (docker-compose, Makefile, CI)

## Checklist

- [ ] `make lint` và `make test` xanh (CI sẽ chạy lại)
- [ ] Nếu đụng dbt: `make dbt-parse` xanh
- [ ] Nếu đụng generator: đã chạy `make generate` và commit `data/samples` mới (tổng < 2 MB)
- [ ] Nội dung cho học viên viết tiếng Việt (giữ thuật ngữ tiếng Anh); tên biến/hàm/commit tiếng Anh
- [ ] Lab có mục **Kết quả mong đợi** và vẫn đúng sau thay đổi
- [ ] Không có URL YouTube bịa — dùng `TODO:youtube-link`
- [ ] Commit message theo [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`…)

## Liên quan

<!-- Closes #123 -->
