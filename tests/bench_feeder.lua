-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
-- helper for bench.lua: serves N pre-built MSG/HMSG frames, or sinks data
local socket = require("socket")
local port, kind, N = tonumber(arg[1]), arg[2], tonumber(arg[3])
local srv = assert(socket.bind("127.0.0.1", port))
local c = srv:accept()
if kind == "SINK" then
    while true do
        local d, err, p = c:receive(65536)
        if not d and err == "closed" then break end
    end
else
    local payload = string.rep("x", 64)
    local t = {}
    for i = 1, N do
        if kind == "MSG" then
            t[#t + 1] = "MSG bench.subject 7 64\r\n" .. payload .. "\r\n"
        else
            local h = "NATS/1.0\r\nX-Id: " .. i .. "\r\n\r\n"
            t[#t + 1] = "HMSG bench.subject 7 " .. #h .. " " .. (#h + 64) .. "\r\n" .. h .. payload .. "\r\n"
        end
        if #t == 1000 then c:send(table.concat(t)); t = {} end
    end
    if #t > 0 then c:send(table.concat(t)) end
    c:receive("*a")
end
c:close()
