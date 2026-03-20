import * as THREE from 'three';
import { createMaterials } from './materials';

// 40ft high-cube container dimensions (meters)
const CL = 12.192; // length
const CW = 2.438; // width
const CH = 2.591; // height

// House = two containers side by side
export const HOUSE_LENGTH = CL;
export const HOUSE_WIDTH = CW * 2; // 4.876
export const HOUSE_HEIGHT = CH;

// Room definitions for interaction
export interface RoomDef {
  name: string;
  bounds: { minX: number; maxX: number; minZ: number; maxZ: number };
  labelPos: THREE.Vector3;
  description: string;
  area: string;
  color: number;
}

// Living room = 1.5x bedroom: 2b + 1.5b = CL → b = CL/3.5
const BED_LEN = CL / 3.5; // 3.484m
const LR_LEN = 1.5 * BED_LEN; // 5.226m
const LR_START = BED_LEN; // 3.484
const LR_END = BED_LEN + LR_LEN; // 8.710

export const ROOMS: RoomDef[] = [
  {
    name: 'Bedroom 1',
    bounds: { minX: 0, maxX: LR_START, minZ: 0, maxZ: HOUSE_WIDTH },
    labelPos: new THREE.Vector3(BED_LEN / 2, 1.5, HOUSE_WIDTH / 2),
    description: 'Bedroom with window view',
    area: `${(BED_LEN * HOUSE_WIDTH).toFixed(1)} m²`,
    color: 0x2196f3,
  },
  {
    name: 'Living Room',
    bounds: { minX: LR_START, maxX: LR_END, minZ: 0, maxZ: HOUSE_WIDTH },
    labelPos: new THREE.Vector3((LR_START + LR_END) / 2, 1.5, HOUSE_WIDTH / 2),
    description: 'Open-plan living area with sliding glass door to veranda',
    area: `${(LR_LEN * HOUSE_WIDTH).toFixed(1)} m²`,
    color: 0x4caf50,
  },
  {
    name: 'Bedroom 2',
    bounds: { minX: LR_END, maxX: CL, minZ: 0, maxZ: HOUSE_WIDTH },
    labelPos: new THREE.Vector3((LR_END + CL) / 2, 1.5, HOUSE_WIDTH / 2),
    description: 'Master bedroom with panoramic windows',
    area: `${(BED_LEN * HOUSE_WIDTH).toFixed(1)} m²`,
    color: 0x9c27b0,
  },
];

// Dimension labels to show
export interface DimensionDef {
  start: THREE.Vector3;
  end: THREE.Vector3;
  label: string;
  offset: THREE.Vector3; // offset for label position from midpoint
}

export const DIMENSIONS: DimensionDef[] = [
  {
    start: new THREE.Vector3(0, 0, -0.5),
    end: new THREE.Vector3(CL, 0, -0.5),
    label: `${CL.toFixed(2)}m`,
    offset: new THREE.Vector3(0, 0.3, 0),
  },
  {
    start: new THREE.Vector3(-0.5, 0, 0),
    end: new THREE.Vector3(-0.5, 0, HOUSE_WIDTH),
    label: `${HOUSE_WIDTH.toFixed(2)}m`,
    offset: new THREE.Vector3(0, 0.3, 0),
  },
  {
    start: new THREE.Vector3(CL + 0.3, 0, 0),
    end: new THREE.Vector3(CL + 0.3, CH, 0),
    label: `${CH.toFixed(2)}m`,
    offset: new THREE.Vector3(0.3, 0, 0),
  },
];

// ---- Wall builder helpers ----

interface Opening {
  left: number;
  right: number;
  bottom: number;
  top: number;
}

function generateWallPanels(
  wallWidth: number,
  wallHeight: number,
  openings: Opening[]
): { x: number; y: number; w: number; h: number }[] {
  const ySet = new Set<number>([0, wallHeight]);
  const xSet = new Set<number>([0, wallWidth]);

  for (const o of openings) {
    ySet.add(o.bottom);
    ySet.add(o.top);
    xSet.add(o.left);
    xSet.add(o.right);
  }

  const ys = Array.from(ySet).sort((a, b) => a - b);
  const xs = Array.from(xSet).sort((a, b) => a - b);
  const panels: { x: number; y: number; w: number; h: number }[] = [];

  for (let i = 0; i < ys.length - 1; i++) {
    const yBot = ys[i];
    const yTop = ys[i + 1];
    const h = yTop - yBot;

    for (let j = 0; j < xs.length - 1; j++) {
      const xLeft = xs[j];
      const xRight = xs[j + 1];
      const w = xRight - xLeft;

      if (w < 0.001 || h < 0.001) continue;

      // Check if this cell is inside any opening
      const isOpen = openings.some(
        (o) =>
          xLeft >= o.left - 0.001 &&
          xRight <= o.right + 0.001 &&
          yBot >= o.bottom - 0.001 &&
          yTop <= o.top + 0.001
      );

      if (!isOpen) {
        panels.push({
          x: (xLeft + xRight) / 2,
          y: (yBot + yTop) / 2,
          w,
          h,
        });
      }
    }
  }

  return panels;
}

function buildWall(
  wallWidth: number,
  wallHeight: number,
  openings: Opening[],
  wallMaterial: THREE.Material,
  glassMaterial: THREE.Material,
  frameMaterial: THREE.Material,
  bumpRepeatScale = 6
): THREE.Group {
  const group = new THREE.Group();

  // Wall panels
  const panels = generateWallPanels(wallWidth, wallHeight, openings);
  for (const p of panels) {
    const geo = new THREE.PlaneGeometry(p.w, p.h);
    const mesh = new THREE.Mesh(geo, wallMaterial);
    mesh.position.set(p.x - wallWidth / 2, p.y, 0);
    mesh.castShadow = true;
    mesh.receiveShadow = true;
    group.add(mesh);
  }

  // Glass panes and frames for each opening
  const fw = 0.05; // frame width
  for (const o of openings) {
    const w = o.right - o.left;
    const h = o.top - o.bottom;
    const cx = (o.left + o.right) / 2 - wallWidth / 2;
    const cy = (o.bottom + o.top) / 2;

    // Glass
    const glass = new THREE.Mesh(new THREE.PlaneGeometry(w - 0.04, h - 0.04), glassMaterial);
    glass.position.set(cx, cy, 0.005);
    group.add(glass);

    // Frame - top
    const topF = new THREE.Mesh(new THREE.BoxGeometry(w + fw * 2, fw, fw), frameMaterial);
    topF.position.set(cx, o.top, 0);
    topF.castShadow = true;
    group.add(topF);

    // Frame - bottom
    const botF = new THREE.Mesh(new THREE.BoxGeometry(w + fw * 2, fw, fw), frameMaterial);
    botF.position.set(cx, o.bottom, 0);
    botF.castShadow = true;
    group.add(botF);

    // Frame - left
    const leftF = new THREE.Mesh(new THREE.BoxGeometry(fw, h, fw), frameMaterial);
    leftF.position.set(o.left - wallWidth / 2, cy, 0);
    leftF.castShadow = true;
    group.add(leftF);

    // Frame - right
    const rightF = new THREE.Mesh(new THREE.BoxGeometry(fw, h, fw), frameMaterial);
    rightF.position.set(o.right - wallWidth / 2, cy, 0);
    rightF.castShadow = true;
    group.add(rightF);

    // Center divider for large windows
    if (w > 1.8) {
      const div = new THREE.Mesh(new THREE.BoxGeometry(fw * 0.6, h, fw), frameMaterial);
      div.position.set(cx, cy, 0);
      group.add(div);
    }

    // Horizontal mullion for tall windows
    if (h > 1.5) {
      const mullion = new THREE.Mesh(new THREE.BoxGeometry(w, fw * 0.6, fw), frameMaterial);
      mullion.position.set(cx, cy, 0);
      group.add(mullion);
    }
  }

  return group;
}

// ---- Main builder ----

export function buildContainerHouse(): THREE.Group {
  const house = new THREE.Group();
  const mat = createMaterials();
  const brickFront = mat.brick(6, 3);
  const brickSide = mat.brick(2, 3);
  const brickBack = mat.brick(6, 3);

  // ============= FOUNDATION =============
  const foundation = new THREE.Group();

  // Concrete slab under house
  const slab = new THREE.Mesh(
    new THREE.BoxGeometry(HOUSE_LENGTH + 0.4, 0.15, HOUSE_WIDTH + 0.4),
    mat.concrete
  );
  slab.position.set(HOUSE_LENGTH / 2, -0.075, HOUSE_WIDTH / 2);
  slab.receiveShadow = true;
  foundation.add(slab);


  house.add(foundation);

  // ============= EXTERIOR WALLS =============

  // --- Front wall (Z=0, facing -Z) ---
  const frontOpenings: Opening[] = [
    { left: 1.0, right: 3.5, bottom: 0.3, top: 2.3 }, // Bedroom 1 window
    { left: (LR_START + LR_END) / 2 - 0.7 * (LR_LEN - 0.6) / 2,
      right: (LR_START + LR_END) / 2 + 0.7 * (LR_LEN - 0.6) / 2,
      bottom: 0, top: 2.3 }, // Sliding glass doors (living room, 70% width, centered)
    { left: LR_END + 0.5, right: CL - 0.5, bottom: 0.3, top: 2.3 }, // Bedroom 2 window
  ];
  const frontWall = buildWall(
    HOUSE_LENGTH,
    HOUSE_HEIGHT,
    frontOpenings,
    brickFront,
    mat.glass,
    mat.steelFrame
  );
  frontWall.rotation.y = Math.PI; // face outward (-Z)
  frontWall.position.set(HOUSE_LENGTH / 2, 0, 0);
  house.add(frontWall);

  // ---- French door for living room (steel grid pattern) ----
  const doorFrameMat = new THREE.MeshStandardMaterial({
    color: 0x2c3e50, // dark navy-charcoal like the screenshot
    metalness: 0.6,
    roughness: 0.35,
  });
  const doorGroup = new THREE.Group();

  const doorL = (LR_START + LR_END) / 2 - 0.7 * (LR_LEN - 0.6) / 2; // door left X (70%, centered)
  const doorR = (LR_START + LR_END) / 2 + 0.7 * (LR_LEN - 0.6) / 2; // door right X
  const doorB = 0;   // bottom
  const doorT = 2.3; // top
  const doorW = doorR - doorL; // 2.9m
  const doorH = doorT - doorB; // 2.3m
  const doorCX = (doorL + doorR) / 2;
  const doorZ = -0.02; // slightly in front of the wall

  const barW = 0.055; // frame bar width
  const barD = 0.05;  // frame bar depth

  // Outer frame (thick border around entire opening)
  const outerW = 0.07;
  // Top
  const doorFTop = new THREE.Mesh(new THREE.BoxGeometry(doorW + outerW, outerW, barD), doorFrameMat);
  doorFTop.position.set(doorCX, doorT, doorZ);
  doorFTop.castShadow = true;
  doorGroup.add(doorFTop);
  // Bottom
  const doorFBot = new THREE.Mesh(new THREE.BoxGeometry(doorW + outerW, outerW, barD), doorFrameMat);
  doorFBot.position.set(doorCX, doorB, doorZ);
  doorFBot.castShadow = true;
  doorGroup.add(doorFBot);
  // Left
  const doorFLeft = new THREE.Mesh(new THREE.BoxGeometry(outerW, doorH, barD), doorFrameMat);
  doorFLeft.position.set(doorL, doorH / 2, doorZ);
  doorFLeft.castShadow = true;
  doorGroup.add(doorFLeft);
  // Right
  const doorFRight = new THREE.Mesh(new THREE.BoxGeometry(outerW, doorH, barD), doorFrameMat);
  doorFRight.position.set(doorR, doorH / 2, doorZ);
  doorFRight.castShadow = true;
  doorGroup.add(doorFRight);

  // 3 vertical dividers (creating 4 panels)
  const panelW = doorW / 4;
  for (let i = 1; i <= 3; i++) {
    const x = doorL + panelW * i;
    const bar = new THREE.Mesh(new THREE.BoxGeometry(barW, doorH, barD), doorFrameMat);
    bar.position.set(x, doorH / 2, doorZ);
    bar.castShadow = true;
    doorGroup.add(bar);
  }

  // 2 horizontal dividers: kick panel line at Y=0.4, transom line at Y=1.85
  const hBarYs = [0.4, 1.85];
  for (const y of hBarYs) {
    const bar = new THREE.Mesh(new THREE.BoxGeometry(doorW, barW, barD), doorFrameMat);
    bar.position.set(doorCX, y, doorZ);
    bar.castShadow = true;
    doorGroup.add(bar);
  }

  house.add(doorGroup);

  // --- Back wall (Z=HOUSE_WIDTH, facing +Z) ---
  // Slim windows on back wall, in the living room section, toward the ends
  const SLIM_W = 0.4;
  const SLIM_B = 0.3;
  const SLIM_T = 2.1;
  const slimInset = 0.5; // distance from interior wall to window edge
  const backOpenings: Opening[] = [
    { left: LR_START + slimInset, right: LR_START + slimInset + SLIM_W, bottom: SLIM_B, top: SLIM_T }, // Left slim window
    { left: LR_END - slimInset - SLIM_W, right: LR_END - slimInset, bottom: SLIM_B, top: SLIM_T }, // Right slim window
  ];
  const backWall = buildWall(
    HOUSE_LENGTH,
    HOUSE_HEIGHT,
    backOpenings,
    brickBack,
    mat.glass,
    mat.steelFrame
  );
  backWall.position.set(HOUSE_LENGTH / 2, 0, HOUSE_WIDTH);
  house.add(backWall);

  // Extra horizontal mullions for the slim back windows (3 bars = 4 panes each)
  const slimMullionMat = mat.steelFrame;
  const slimMullionBarW = 0.04;
  const slimH = SLIM_T - SLIM_B;
  for (const opening of backOpenings) {
    const cx = (opening.left + opening.right) / 2;
    const w = opening.right - opening.left;
    for (let j = 1; j <= 3; j++) {
      const y = SLIM_B + (slimH / 4) * j;
      const mullion = new THREE.Mesh(
        new THREE.BoxGeometry(w, slimMullionBarW, 0.04),
        slimMullionMat
      );
      mullion.position.set(cx, y, HOUSE_WIDTH + 0.01);
      house.add(mullion);
    }
  }

  // --- Left wall (X=0, facing -X) ---
  const leftOpenings: Opening[] = [
    { left: 0.9125, right: 3.3875, bottom: 0.55, top: 2.05 }, // Bedroom 1 side window (identical to Bedroom 2)
  ];
  const leftWall = buildWall(HOUSE_WIDTH, HOUSE_HEIGHT, leftOpenings, brickSide, mat.glass, mat.steelFrame);
  leftWall.rotation.y = Math.PI / 2; // face -X
  leftWall.position.set(0, 0, HOUSE_WIDTH / 2);
  house.add(leftWall);

  // --- Right wall (X=HOUSE_LENGTH, facing +X) ---
  const rightOpenings: Opening[] = [
    { left: 0.9125, right: 3.3875, bottom: 0.55, top: 2.05 }, // Bedroom 2 side window (75% size)
  ];
  const rightWall = buildWall(
    HOUSE_WIDTH,
    HOUSE_HEIGHT,
    rightOpenings,
    brickSide,
    mat.glass,
    mat.steelFrame
  );
  rightWall.rotation.y = -Math.PI / 2; // face +X
  rightWall.position.set(HOUSE_LENGTH, 0, HOUSE_WIDTH / 2);
  house.add(rightWall);

  // ============= ROOF & FLOOR =============

  // Mid-century modern overhanging roof - slants toward the back
  const roofGroup = new THREE.Group();
  const OVERHANG_FRONT = 0.32; // 10% of veranda deck depth
  const OVERHANG_BACK = 0.4;
  const OVERHANG_LEFT = 0.6;
  const OVERHANG_RIGHT = 0.6;
  const ROOF_SLOPE = 0.3; // front is higher, slopes down toward back
  const ROOF_THICKNESS = 0.14;

  const roofLeft = -OVERHANG_LEFT;
  const roofRight = HOUSE_LENGTH + OVERHANG_RIGHT;
  const roofFront = -OVERHANG_FRONT;
  const roofBack = HOUSE_WIDTH + OVERHANG_BACK;
  const roofW = roofRight - roofLeft;
  const roofD = roofBack - roofFront;

  const roofY_front = HOUSE_HEIGHT + 0.1 + ROOF_SLOPE; // higher at front
  const roofY_back = HOUSE_HEIGHT + 0.1; // lower at back

  // Solid roof slab (dark top surface + dark underside as one thick box-like shape)
  // Top surface
  const roofTopGeo = new THREE.BufferGeometry();
  const topVerts = new Float32Array([
    roofLeft, roofY_front, roofFront,
    roofRight, roofY_front, roofFront,
    roofRight, roofY_back, roofBack,
    roofLeft, roofY_back, roofBack,
  ]);
  roofTopGeo.setAttribute('position', new THREE.BufferAttribute(topVerts, 3));
  roofTopGeo.setIndex([0, 2, 1, 0, 3, 2]);
  roofTopGeo.computeVertexNormals();
  const roofTopMesh = new THREE.Mesh(roofTopGeo, mat.roofTop);
  roofTopMesh.castShadow = true;
  roofTopMesh.receiveShadow = true;
  roofGroup.add(roofTopMesh);

  // Bottom surface (wood soffit, only visible from underneath)
  const underY_front = roofY_front - ROOF_THICKNESS;
  const underY_back = roofY_back - ROOF_THICKNESS;
  const roofBotGeo = new THREE.BufferGeometry();
  const botVerts = new Float32Array([
    roofLeft, underY_front, roofFront,
    roofRight, underY_front, roofFront,
    roofRight, underY_back, roofBack,
    roofLeft, underY_back, roofBack,
  ]);
  roofBotGeo.setAttribute('position', new THREE.BufferAttribute(botVerts, 3));
  roofBotGeo.setIndex([0, 1, 2, 0, 2, 3]);
  roofBotGeo.computeVertexNormals();
  const roofBotMesh = new THREE.Mesh(roofBotGeo, mat.roofWood);
  roofGroup.add(roofBotMesh);

  // Fascia boards (close off the roof edges so it looks solid)
  // Front fascia (tall edge, highest point)
  const frontFascia = new THREE.Mesh(
    new THREE.BoxGeometry(roofW, ROOF_THICKNESS, 0.05),
    mat.roofFascia
  );
  frontFascia.position.set((roofLeft + roofRight) / 2, roofY_front - ROOF_THICKNESS / 2, roofFront);
  frontFascia.castShadow = true;
  roofGroup.add(frontFascia);

  // Back fascia (lower edge)
  const backFascia = new THREE.Mesh(
    new THREE.BoxGeometry(roofW, ROOF_THICKNESS, 0.05),
    mat.roofFascia
  );
  backFascia.position.set((roofLeft + roofRight) / 2, roofY_back - ROOF_THICKNESS / 2, roofBack);
  backFascia.castShadow = true;
  roofGroup.add(backFascia);



  house.add(roofGroup);

  // Interior floor
  const floor = new THREE.Mesh(
    new THREE.PlaneGeometry(HOUSE_LENGTH, HOUSE_WIDTH),
    mat.interiorFloor
  );
  floor.rotation.x = -Math.PI / 2;
  floor.position.set(HOUSE_LENGTH / 2, 0.01, HOUSE_WIDTH / 2);
  floor.receiveShadow = true;
  floor.name = 'interiorFloor';
  house.add(floor);

  // ============= INTERIOR WALLS =============

  // Interior door dimensions
  const INT_DOOR_W = 0.9; // door width
  const INT_DOOR_H = 2.1; // door height
  const INT_DOOR_FRAME = 0.15; // small frame strip on the front side

  // Divider: Bedroom 1 | Living Room at X=LR_START
  // Door positioned near the front wall (Z=0 side)
  // Top strip (above door, full width)
  const div1Top = new THREE.Mesh(
    new THREE.PlaneGeometry(HOUSE_WIDTH, HOUSE_HEIGHT - INT_DOOR_H), mat.interiorWall);
  div1Top.rotation.y = Math.PI / 2;
  div1Top.position.set(LR_START, INT_DOOR_H + (HOUSE_HEIGHT - INT_DOOR_H) / 2, HOUSE_WIDTH / 2);
  house.add(div1Top);

  // Small strip between front wall and door
  const div1Front = new THREE.Mesh(
    new THREE.PlaneGeometry(INT_DOOR_FRAME, INT_DOOR_H), mat.interiorWall);
  div1Front.rotation.y = Math.PI / 2;
  div1Front.position.set(LR_START, INT_DOOR_H / 2, INT_DOOR_FRAME / 2);
  house.add(div1Front);

  // Large panel from door right edge to back wall
  const div1BackW = HOUSE_WIDTH - INT_DOOR_FRAME - INT_DOOR_W;
  const div1Back = new THREE.Mesh(
    new THREE.PlaneGeometry(div1BackW, INT_DOOR_H), mat.interiorWall);
  div1Back.rotation.y = Math.PI / 2;
  div1Back.position.set(LR_START, INT_DOOR_H / 2, INT_DOOR_FRAME + INT_DOOR_W + div1BackW / 2);
  house.add(div1Back);

  // Divider: Living Room | Bedroom 2 at X=LR_END
  // Door positioned near the front wall (Z=0 side), identical layout
  const div2Top = new THREE.Mesh(
    new THREE.PlaneGeometry(HOUSE_WIDTH, HOUSE_HEIGHT - INT_DOOR_H), mat.interiorWall);
  div2Top.rotation.y = Math.PI / 2;
  div2Top.position.set(LR_END, INT_DOOR_H + (HOUSE_HEIGHT - INT_DOOR_H) / 2, HOUSE_WIDTH / 2);
  house.add(div2Top);

  const div2Front = new THREE.Mesh(
    new THREE.PlaneGeometry(INT_DOOR_FRAME, INT_DOOR_H), mat.interiorWall);
  div2Front.rotation.y = Math.PI / 2;
  div2Front.position.set(LR_END, INT_DOOR_H / 2, INT_DOOR_FRAME / 2);
  house.add(div2Front);

  const div2BackW = HOUSE_WIDTH - INT_DOOR_FRAME - INT_DOOR_W;
  const div2Back = new THREE.Mesh(
    new THREE.PlaneGeometry(div2BackW, INT_DOOR_H), mat.interiorWall);
  div2Back.rotation.y = Math.PI / 2;
  div2Back.position.set(LR_END, INT_DOOR_H / 2, INT_DOOR_FRAME + INT_DOOR_W + div2BackW / 2);
  house.add(div2Back);

  // ============= VERANDA DECK =============
  const veranda = new THREE.Group();
  const DECK_LENGTH = HOUSE_LENGTH; // spans full front
  const DECK_DEPTH = 3.2; // extends 3.2m from front wall
  const DECK_HEIGHT = 0.0; // flush with interior floor
  const PLANK_WIDTH = 0.15;
  const PLANK_GAP = 0.01;

  // Deck planks (individual boards for realism)
  const plankStep = PLANK_WIDTH + PLANK_GAP;
  const plankCount = Math.floor(DECK_DEPTH / plankStep);
  for (let i = 0; i < plankCount; i++) {
    const plank = new THREE.Mesh(
      new THREE.BoxGeometry(DECK_LENGTH, 0.025, PLANK_WIDTH),
      mat.deckWood
    );
    plank.position.set(
      DECK_LENGTH / 2,
      DECK_HEIGHT - 0.01,
      -(i * plankStep + PLANK_WIDTH / 2 + 0.1)
    );
    plank.receiveShadow = true;
    veranda.add(plank);
  }

  // Deck subframe (support beams underneath)
  const beamCount = 4;
  for (let i = 0; i < beamCount; i++) {
    const x = (DECK_LENGTH / (beamCount + 1)) * (i + 1);
    const beam = new THREE.Mesh(
      new THREE.BoxGeometry(0.1, 0.12, DECK_DEPTH),
      mat.deckWood
    );
    beam.position.set(x, -0.08, -DECK_DEPTH / 2);
    veranda.add(beam);
  }


  house.add(veranda);

  // ============= GROUND & LANDSCAPE =============
  const landscape = new THREE.Group();

  // Large ground plane
  const ground = new THREE.Mesh(new THREE.PlaneGeometry(60, 60), mat.grass);
  ground.rotation.x = -Math.PI / 2;
  ground.position.set(HOUSE_LENGTH / 2, -0.15, HOUSE_WIDTH / 2);
  ground.receiveShadow = true;
  landscape.add(ground);

  // Stepping stones (path from front of veranda)
  const stoneGeo = new THREE.BoxGeometry(0.5, 0.05, 0.4);
  for (let i = 0; i < 6; i++) {
    const stone = new THREE.Mesh(stoneGeo, mat.steppingStone);
    stone.position.set(
      HOUSE_LENGTH / 2 + Math.sin(i * 0.4) * 0.3,
      -0.12,
      -4.5 - i * 1.2
    );
    stone.rotation.y = Math.random() * 0.3 - 0.15;
    stone.receiveShadow = true;
    landscape.add(stone);
  }

  house.add(landscape);


  // ============= ROOM FLOOR MARKERS (invisible, for raycasting) =============
  for (const room of ROOMS) {
    const w = room.bounds.maxX - room.bounds.minX;
    const d = room.bounds.maxZ - room.bounds.minZ;
    const cx = (room.bounds.minX + room.bounds.maxX) / 2;
    const cz = (room.bounds.minZ + room.bounds.maxZ) / 2;

    const marker = new THREE.Mesh(
      new THREE.PlaneGeometry(w, d),
      new THREE.MeshBasicMaterial({ visible: false })
    );
    marker.rotation.x = -Math.PI / 2;
    marker.position.set(cx, 0.02, cz);
    marker.name = `room_${room.name}`;
    marker.userData = { room };
    house.add(marker);
  }

  // Center the house so it looks good in the scene
  // Offset so the house center is roughly at origin
  house.position.set(-HOUSE_LENGTH / 2, 0, -HOUSE_WIDTH / 2);

  return house;
}
