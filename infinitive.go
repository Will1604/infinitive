package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type TStatZoneConfig struct {
	TempUnit        string `json:"tempUnit"`
	CurrentTemp     uint8  `json:"currentTemp"`
	CurrentHumidity uint8  `json:"currentHumidity"`
	OutdoorTemp     int8   `json:"outdoorTemp"`
	Mode            string `json:"mode"`
	Stage           uint8  `json:"stage"`
	FanMode         string `json:"fanMode"`
	Hold            *bool  `json:"hold"`
	HoldDuration    uint16 `json:"holdDurationMins"`
	HeatSetpoint    uint8  `json:"heatSetpoint"`
	CoolSetpoint    uint8  `json:"coolSetpoint"`
	TargetHum       uint8  `json:"targetHumidity"`
	RawMode         uint8  `json:"rawMode"`
}

type AirHandler struct {
	BlowerRPM  uint16 `json:"blowerRPM"`
	AirFlowCFM uint16 `json:"airFlowCFM"`
	HeatBits   string `json:"heatBits"`
	AuxHeat    bool   `json:"auxHeat"`
}

type HeatPump struct {
	TempUnit    string  `json:"tempUnit"`
	CoilTemp    float32 `json:"coilTemp"`
	OutsideTemp float32 `json:"outsideTemp"`
	Stage       uint8   `json:"stage"`
}

type DeviceInfo struct {
	Description string `json:"description"`
	Product     string `json:"product"`
	Software    string `json:"softwareVersion"`
	//	Unknown     string `json:"unknown"`
	//	SerialNo    string `json:"serialNo"`
	BusId string `json:"busId"`
}

func (dev *DeviceInfo) read104(src uint16, data []byte) {
	dev.BusId = fmt.Sprintf("%4x", src)
	dev.Description = devInfo(data[0:48])
	dev.Software = devInfo(data[48:64])
	dev.Product = devInfo(data[64:84])
	//  Leaving out potentially identifying information, for now at least
	//	dev.Unknown = devInfo(data[84:96])
	//	dev.SerialNo = devInfo(data[96:])
}

func devInfo(data []byte) string {
	b := bytes.NewBuffer(data)
	info, err := b.ReadString(0)
	if err != nil {
		log.Printf("Error reading device info|%s|", err.Error())
		log.Printf("Data was:%x", data)
	}
	return strings.TrimRight(info, " \x00")
}

type DeviceList struct {
	Thermostat DeviceInfo `json:"thermostat"`
	AirHandler DeviceInfo `json:"airhandler"`
	HeatPump   DeviceInfo `json:"heatpump"`
}

var infinity *InfinityProtocol

func getConfig() (*TStatZoneConfig, bool) {
	cfg := TStatZoneParams{}
	ok := infinity.ReadTable(devTSTAT, &cfg)
	if !ok {
		return nil, false
	}

	params := TStatCurrentParams{}
	ok = infinity.ReadTable(devTSTAT, &params)
	if !ok {
		return nil, false
	}

	hold := new(bool)
	*hold = cfg.ZoneHold&0x01 == 1

	return &TStatZoneConfig{
		TempUnit:        infinity.tempUnitStr(),
		CurrentTemp:     params.Z1CurrentTemp,
		CurrentHumidity: params.Z1CurrentHumidity,
		OutdoorTemp:     params.OutdoorAirTemp,
		Mode:            rawModeToString(params.Mode & 0xf),
		Stage:           params.Mode >> 5,
		FanMode:         rawFanModeToString(cfg.Z1FanMode),
		Hold:            hold,
		HoldDuration:    cfg.Z1HoldDuration,
		HeatSetpoint:    cfg.Z1HeatSetpoint,
		CoolSetpoint:    cfg.Z1CoolSetpoint,
		TargetHum:       cfg.Z1TargetHumidity,
		RawMode:         params.Mode,
	}, true
}

func getDeviceInfo(address uint16) (*DeviceInfo, bool) {
	params := DevInfoParams{}
	ok := infinity.ReadTable(address, &params)
	if !ok {
		return nil, false
	}

	return &DeviceInfo{
		BusId:       fmt.Sprintf("%4x", address),
		Description: devInfo(params.Description[0:]),
		Software:    devInfo(params.Software[0:]),
		Product:     devInfo(params.Product[0:]),
	}, true
}

func getAirHandler() (AirHandler, bool) {
	b := cache.get("blower")
	tb, ok := b.(*AirHandler)
	if !ok {
		return AirHandler{}, false
	}
	return *tb, true
}

func getHeatPump() (HeatPump, bool) {
	h := cache.get("heatpump")
	th, ok := h.(*HeatPump)
	if !ok {
		return HeatPump{}, false
	}
	return *th, true
}

func getDevices() (DeviceList, bool) {
	d := cache.get("devices")
	td, ok := d.(*DeviceList)
	if !ok {
		return DeviceList{}, false
	}
	return *td, true
}

func statePoller() {
	for {
		c, ok := getConfig()
		if ok {
			cache.update("tstat", c)
		}
		time.Sleep(time.Second * 1)
	}
}

func attachSnoops() {

	// Snoop Heat Pump responses
	infinity.snoopResponse(0x5000, 0x51ff, func(frame *InfinityFrame) {
		data := frame.data[3:]
		heatPump, ok := getHeatPump()
		if ok {
			if bytes.Equal(frame.data[0:3], []byte{0x00, 0x3e, 0x01}) {
				heatPump.CoilTemp = float32(binary.BigEndian.Uint16(data[2:4])) / float32(16)
				heatPump.OutsideTemp = float32(binary.BigEndian.Uint16(data[0:2])) / float32(16)
				heatPump.TempUnit = infinity.tempUnitStr()
				if heatPump.TempUnit == "C" {
					// heat pump data on the bus always seems to be in Fahrenheit
					heatPump.CoilTemp = tempFtoC(heatPump.CoilTemp)
					heatPump.OutsideTemp = tempFtoC(heatPump.OutsideTemp)
				}
				log.Debugf("heat pump coil temp is: %f %s", heatPump.CoilTemp, heatPump.TempUnit)
				log.Debugf("heat pump outside temp is: %f %s", heatPump.OutsideTemp, heatPump.TempUnit)
				cache.update("heatpump", &heatPump)
			} else if bytes.Equal(frame.data[0:3], []byte{0x00, 0x3e, 0x02}) {
				heatPump.Stage = data[0] >> 1
				log.Debugf("HP stage is: %d", heatPump.Stage)
				cache.update("heatpump", &heatPump)
			} else if bytes.Equal(frame.data[0:3], []byte{0x00, 0x3e, 0x08}) {
				deviceList, ok := getDevices()
				if ok {
					b := bytes.NewBuffer(data)
					deviceList.HeatPump.Product = b.String()
					deviceList.HeatPump.BusId = fmt.Sprintf("%4x", frame.src)
					cache.update("devices", &deviceList)
				}
			} else if bytes.Equal(frame.data[0:3], []byte{0x00, 0x01, 0x04}) {
				deviceList, ok := getDevices()
				if ok {
					deviceList.HeatPump.read104(frame.src, data)
					cache.update("devices", &deviceList)
				}
			}
		}
	})

	// Snoop Air Handler responses
	infinity.snoopResponse(0x4000, 0x42ff, func(frame *InfinityFrame) {
		data := frame.data[3:]
		airHandler, ok := getAirHandler()
		if ok {
			if bytes.Equal(frame.data[0:3], []byte{0x00, 0x03, 0x06}) {
				airHandler.BlowerRPM = binary.BigEndian.Uint16(data[1:5])
				log.Debugf("blower RPM is: %d", airHandler.BlowerRPM)
				cache.update("blower", &airHandler)
			} else if bytes.Equal(frame.data[0:3], []byte{0x00, 0x03, 0x16}) {
				airHandler.AirFlowCFM = binary.BigEndian.Uint16(data[4:8])
				airHandler.HeatBits = fmt.Sprintf("%08b", data[0])
				airHandler.AuxHeat = (data[0]&0x03 != 0)
				log.Debugf("air flow CFM is: %d", airHandler.AirFlowCFM)
				cache.update("blower", &airHandler)
			} else if bytes.Equal(frame.data[0:3], []byte{0x00, 0x01, 0x04}) {
				deviceList, ok := getDevices()
				if ok {
					deviceList.AirHandler.read104(frame.src, data)
					cache.update("devices", &deviceList)
				}
			}
		}
	})

}

func main() {
	httpPort := flag.Int("httpport", 8080, "HTTP port to listen on")
	serialPort := flag.String("serial", "", "path to serial port")

	flag.Parse()

	if len(*serialPort) == 0 {
		fmt.Print("must provide serial\n")
		flag.PrintDefaults()
		os.Exit(1)
	}

	log.SetLevel(log.DebugLevel)

	infinity = &InfinityProtocol{device: *serialPort, reportDuration: time.Hour * 24}
	airHandler := new(AirHandler)
	heatPump := new(HeatPump)
	devices := new(DeviceList)
	cache.update("blower", airHandler)
	cache.update("heatpump", heatPump)
	cache.update("devices", devices)
	attachSnoops()
	err := infinity.Open()
	if err != nil {
		log.Panicf("error opening serial port: %s", err.Error())
	}

	go statePoller()
	webserver(*httpPort)
}
