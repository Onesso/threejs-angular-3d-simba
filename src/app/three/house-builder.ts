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
  color?: string; // label background color (default: red for house, blue for windows/doors)
}

// Window/door dimensions for labels
const SIDE_WIN_W = 3.3875 - 0.9125; // 2.475m
const SIDE_WIN_H = 2.05 - 0.55; // 1.5m
const DOOR_W_VAL = 0.7 * (1.5 * (CL / 3.5) - 0.6); // ~3.24m
const DOOR_H_VAL = 2.3;
const SLIM_WIN_W_VAL = 0.4;
const SLIM_WIN_H_VAL = 2.1 - 0.3; // 1.8m

// Positions for front window/door labels
const _bed1CX = (CL / 3.5) / 2;
const _lrStart = CL / 3.5;
const _lrEnd = CL / 3.5 + 1.5 * (CL / 3.5);
const _bed2CX = _lrEnd + (CL / 3.5) / 2;
const _doorCX = (_lrStart + _lrEnd) / 2;

export const DIMENSIONS: DimensionDef[] = [
  // House overall
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
  // Bedroom 1 front window
  {
    start: new THREE.Vector3(_bed1CX - SIDE_WIN_W / 2, 2.15, -0.3),
    end: new THREE.Vector3(_bed1CX + SIDE_WIN_W / 2, 2.15, -0.3),
    label: `${SIDE_WIN_W.toFixed(2)}m`,
    offset: new THREE.Vector3(0, 0.2, 0),
    color: '#1565c0',
  },
  {
    start: new THREE.Vector3(_bed1CX - SIDE_WIN_W / 2 - 0.15, 0.55, -0.3),
    end: new THREE.Vector3(_bed1CX - SIDE_WIN_W / 2 - 0.15, 2.05, -0.3),
    label: `${SIDE_WIN_H.toFixed(2)}m`,
    offset: new THREE.Vector3(-0.25, 0, 0),
    color: '#1565c0',
  },
  // Front door
  {
    start: new THREE.Vector3(_doorCX - DOOR_W_VAL / 2, 2.4, -0.3),
    end: new THREE.Vector3(_doorCX + DOOR_W_VAL / 2, 2.4, -0.3),
    label: `${DOOR_W_VAL.toFixed(2)}m`,
    offset: new THREE.Vector3(0, 0.2, 0),
    color: '#2e7d32',
  },
  {
    start: new THREE.Vector3(_doorCX + DOOR_W_VAL / 2 + 0.15, 0, -0.3),
    end: new THREE.Vector3(_doorCX + DOOR_W_VAL / 2 + 0.15, DOOR_H_VAL, -0.3),
    label: `${DOOR_H_VAL.toFixed(2)}m`,
    offset: new THREE.Vector3(0.25, 0, 0),
    color: '#2e7d32',
  },
  // Bedroom 2 front window
  {
    start: new THREE.Vector3(_bed2CX - SIDE_WIN_W / 2, 2.15, -0.3),
    end: new THREE.Vector3(_bed2CX + SIDE_WIN_W / 2, 2.15, -0.3),
    label: `${SIDE_WIN_W.toFixed(2)}m`,
    offset: new THREE.Vector3(0, 0.2, 0),
    color: '#1565c0',
  },
  // Back slim windows
  {
    start: new THREE.Vector3(_lrStart + 0.5, 2.2, HOUSE_WIDTH + 0.3),
    end: new THREE.Vector3(_lrStart + 0.5 + SLIM_WIN_W_VAL, 2.2, HOUSE_WIDTH + 0.3),
    label: `${SLIM_WIN_W_VAL.toFixed(2)}m`,
    offset: new THREE.Vector3(0, 0.2, 0),
    color: '#1565c0',
  },
  {
    start: new THREE.Vector3(_lrStart + 0.3, 0.3, HOUSE_WIDTH + 0.3),
    end: new THREE.Vector3(_lrStart + 0.3, 2.1, HOUSE_WIDTH + 0.3),
    label: `${SLIM_WIN_H_VAL.toFixed(2)}m`,
    offset: new THREE.Vector3(-0.2, 0, 0),
    color: '#1565c0',
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
  skipGlassIndices: number[] = []
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
  for (let _oi = 0; _oi < openings.length; _oi++) {
    const o = openings[_oi];
    if (skipGlassIndices.includes(_oi)) continue;
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
  // Bedroom front windows: same dimensions as side windows (2.475m x 1.5m), centered in each bedroom
  const sideWinW = 3.3875 - 0.9125; // 2.475m
  const sideWinB = 0.55;
  const sideWinT = 2.05;
  const bed1CX = BED_LEN / 2; // center of bedroom 1
  const bed2CX = LR_END + BED_LEN / 2; // center of bedroom 2
  const frontOpenings: Opening[] = [
    { left: bed1CX - sideWinW / 2, right: bed1CX + sideWinW / 2, bottom: sideWinB, top: sideWinT }, // Bedroom 1 window
    { left: (LR_START + LR_END) / 2 - 0.7 * (LR_LEN - 0.6) / 2,
      right: (LR_START + LR_END) / 2 + 0.7 * (LR_LEN - 0.6) / 2,
      bottom: 0, top: 2.3 }, // Sliding glass doors (living room, 70% width, centered)
    { left: bed2CX - sideWinW / 2, right: bed2CX + sideWinW / 2, bottom: sideWinB, top: sideWinT }, // Bedroom 2 window
  ];
  const frontWall = buildWall(
    HOUSE_LENGTH,
    HOUSE_HEIGHT,
    frontOpenings,
    brickFront,
    mat.glass,
    mat.steelFrame,
    [1] // skip glass/frames for the door opening (handled by French door leaves)
  );
  frontWall.rotation.y = Math.PI; // face outward (-Z)
  frontWall.position.set(HOUSE_LENGTH / 2, 0, 0);
  house.add(frontWall);

  // ---- French door for living room (steel grid pattern) ----
  const doorFrameMat = new THREE.MeshStandardMaterial({
    color: 0x2c3e50, // dark navy-charcoal like the screenshot
    metalness: 0.6,
    roughness: 0.35,
    side: THREE.DoubleSide,
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

  // Only the top lintel frame (no bottom/sides — door is open, passage is clear)
  const outerW = 0.07;
  const doorFTop = new THREE.Mesh(new THREE.BoxGeometry(doorW + outerW, outerW, barD), doorFrameMat);
  doorFTop.position.set(doorCX, doorT, doorZ);
  doorFTop.castShadow = true;
  doorGroup.add(doorFTop);

  // French door: 4 panels, outer 2 fixed, inner 2 swing open from the center
  const panelW = doorW / 4;
  const kickH = 0.4;
  const DOOR_OPEN_ANGLE = Math.PI * 0.4;
  const midH = 1.85 - kickH;
  const topH = doorH - 1.85;

  // Helper: build a single door panel
  const buildPanel = (pw: number): THREE.Group => {
    const panel = new THREE.Group();

    // Vertical frame bars (left and right edges)
    for (const x of [0, pw]) {
      const vbar = new THREE.Mesh(new THREE.BoxGeometry(barW, doorH, barD), doorFrameMat);
      vbar.position.set(x, doorH / 2, 0);
      vbar.castShadow = true;
      panel.add(vbar);
    }

    // Horizontal bars: bottom, kick line, transom line, top
    for (const y of [0, kickH, 1.85, doorH]) {
      const hbar = new THREE.Mesh(new THREE.BoxGeometry(pw, barW, barD), doorFrameMat);
      hbar.position.set(pw / 2, y, 0);
      hbar.castShadow = true;
      panel.add(hbar);
    }

    // Metal kick panel
    const kick = new THREE.Mesh(
      new THREE.PlaneGeometry(pw - barW, kickH - barW),
      doorFrameMat
    );
    kick.position.set(pw / 2, kickH / 2, 0.005);
    panel.add(kick);

    // Middle glass
    const midGlass = new THREE.Mesh(
      new THREE.PlaneGeometry(pw - barW - 0.02, midH - barW - 0.02),
      mat.glass
    );
    midGlass.position.set(pw / 2, kickH + midH / 2, 0.005);
    panel.add(midGlass);

    // Transom glass
    const topGlass = new THREE.Mesh(
      new THREE.PlaneGeometry(pw - barW - 0.02, topH - barW - 0.02),
      mat.glass
    );
    topGlass.position.set(pw / 2, 1.85 + topH / 2, 0.005);
    panel.add(topGlass);

    return panel;
  };

  // Panel 1 (far left) - FIXED
  const panel1 = buildPanel(panelW);
  panel1.position.set(doorL, 0, doorZ);
  doorGroup.add(panel1);

  // Panel 4 (far right) - FIXED
  const panel4 = buildPanel(panelW);
  panel4.scale.x = -1;
  panel4.position.set(doorR, 0, doorZ);
  doorGroup.add(panel4);

  // Panel 2 (left-center) - SWINGS, hinged where it meets panel 1
  const leftLeaf = buildPanel(panelW);
  leftLeaf.position.set(doorL + panelW, 0, doorZ); // hinge at left edge
  leftLeaf.rotation.y = DOOR_OPEN_ANGLE; // start open
  doorGroup.add(leftLeaf);

  // Panel 3 (right-center) - SWINGS, hinged where it meets panel 4
  const rightLeaf = buildPanel(panelW);
  rightLeaf.scale.x = -1;
  rightLeaf.position.set(doorR - panelW, 0, doorZ); // hinge at right edge
  rightLeaf.rotation.y = -DOOR_OPEN_ANGLE; // start open
  doorGroup.add(rightLeaf);

  house.add(doorGroup);

  // Store door references for animation
  house.userData['doorLeftLeaf'] = leftLeaf;
  house.userData['doorRightLeaf'] = rightLeaf;
  house.userData['doorOpenAngle'] = DOOR_OPEN_ANGLE;
  house.userData['doorIsOpen'] = true;

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



  // Fill gap between front wall top and roof underside
  // Front wall is at Z=0, height HOUSE_HEIGHT
  // Roof underside at front: underY_front = roofY_front - ROOF_THICKNESS
  // Need to fill from Y=HOUSE_HEIGHT to Y=underY_front across the full house width
  const frontGapH = underY_front - HOUSE_HEIGHT;
  if (frontGapH > 0.01) {
    const frontFill = new THREE.Mesh(
      new THREE.PlaneGeometry(HOUSE_LENGTH, frontGapH),
      brickFront
    );
    frontFill.rotation.y = Math.PI; // face outward
    frontFill.position.set(HOUSE_LENGTH / 2, HOUSE_HEIGHT + frontGapH / 2, 0);
    frontFill.castShadow = true;
    house.add(frontFill);

    // Interior side of the front fill
    const frontFillIn = new THREE.Mesh(
      new THREE.PlaneGeometry(HOUSE_LENGTH, frontGapH),
      mat.interiorWall
    );
    frontFillIn.position.set(HOUSE_LENGTH / 2, HOUSE_HEIGHT + frontGapH / 2, 0.01);
    house.add(frontFillIn);
  }

  // Fill gap on back wall (roof is lower at back, but still might have a small gap)
  const backGapH = underY_back - HOUSE_HEIGHT;
  if (backGapH > 0.01) {
    const backFill = new THREE.Mesh(
      new THREE.PlaneGeometry(HOUSE_LENGTH, backGapH),
      brickBack
    );
    backFill.position.set(HOUSE_LENGTH / 2, HOUSE_HEIGHT + backGapH / 2, HOUSE_WIDTH);
    backFill.castShadow = true;
    house.add(backFill);
  }

  // Fill left side gap (sloped: higher at front Z=0, lower at back Z=HOUSE_WIDTH)
  const leftGapGeo = new THREE.BufferGeometry();
  const lgVerts = new Float32Array([
    0, HOUSE_HEIGHT, 0,
    0, underY_front, 0,
    0, underY_back, HOUSE_WIDTH,
    0, HOUSE_HEIGHT, HOUSE_WIDTH,
  ]);
  leftGapGeo.setAttribute('position', new THREE.BufferAttribute(lgVerts, 3));
  leftGapGeo.setIndex([0, 1, 2, 0, 2, 3]);
  leftGapGeo.computeVertexNormals();
  const leftGapFill = new THREE.Mesh(leftGapGeo, brickSide);
  leftGapFill.castShadow = true;
  house.add(leftGapFill);

  // Fill right side gap
  const rightGapGeo = new THREE.BufferGeometry();
  const rgVerts = new Float32Array([
    HOUSE_LENGTH, HOUSE_HEIGHT, 0,
    HOUSE_LENGTH, underY_front, 0,
    HOUSE_LENGTH, underY_back, HOUSE_WIDTH,
    HOUSE_LENGTH, HOUSE_HEIGHT, HOUSE_WIDTH,
  ]);
  rightGapGeo.setAttribute('position', new THREE.BufferAttribute(rgVerts, 3));
  rightGapGeo.setIndex([0, 2, 1, 0, 3, 2]); // flipped winding for outward normals
  rightGapGeo.computeVertexNormals();
  const rightGapFill = new THREE.Mesh(rightGapGeo, brickSide);
  rightGapFill.castShadow = true;
  house.add(rightGapFill);

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

  // ============= LIVING ROOM FURNITURE =============
  const lrCX = (LR_START + LR_END) / 2; // center X of living room

  // --- TV mounted on back wall ---
  const tvScreenMat = new THREE.MeshStandardMaterial({
    color: 0x111111, metalness: 0.3, roughness: 0.2,
  });
  const tvFrameMat = new THREE.MeshStandardMaterial({
    color: 0x1a1a1a, metalness: 0.5, roughness: 0.3,
  });

  // TV screen (55 inch ~ 1.22m x 0.69m)
  const TV_W = 1.22;
  const TV_H = 0.69;
  const TV_Y = 1.4; // center height on wall
  const tvScreen = new THREE.Mesh(
    new THREE.BoxGeometry(TV_W, TV_H, 0.03),
    tvScreenMat
  );
  tvScreen.position.set(lrCX, TV_Y, HOUSE_WIDTH - 0.12);
  house.add(tvScreen);

  // TV bezel/frame
  const tvFrame = new THREE.Mesh(
    new THREE.BoxGeometry(TV_W + 0.04, TV_H + 0.04, 0.02),
    tvFrameMat
  );
  tvFrame.position.set(lrCX, TV_Y, HOUSE_WIDTH - 0.14);
  house.add(tvFrame);

  // TV mount bracket (between TV and wall)
  const tvBracket = new THREE.Mesh(
    new THREE.BoxGeometry(0.3, 0.2, 0.08),
    tvFrameMat
  );
  tvBracket.position.set(lrCX, TV_Y, HOUSE_WIDTH - 0.06);
  house.add(tvBracket);

  // --- Sofa facing the TV ---
  const sofaMat = new THREE.MeshStandardMaterial({
    color: 0x4a4a52, roughness: 0.85, metalness: 0.0,
  });
  const sofaCushionMat = new THREE.MeshStandardMaterial({
    color: 0x555560, roughness: 0.9, metalness: 0.0,
  });

  const SOFA_W = 2.0;
  const SOFA_D = 0.85;
  const SOFA_H = 0.4;
  const SOFA_Z = HOUSE_WIDTH / 2 + 0.3; // offset toward TV side

  // Sofa base
  const sofaBase = new THREE.Mesh(
    new THREE.BoxGeometry(SOFA_W, SOFA_H, SOFA_D),
    sofaMat
  );
  sofaBase.position.set(lrCX, SOFA_H / 2, SOFA_Z);
  sofaBase.castShadow = true;
  sofaBase.receiveShadow = true;
  house.add(sofaBase);

  // Sofa backrest (facing the front/door, back toward TV)
  const sofaBack = new THREE.Mesh(
    new THREE.BoxGeometry(SOFA_W, 0.35, 0.15),
    sofaMat
  );
  sofaBack.position.set(lrCX, SOFA_H + 0.175, SOFA_Z - SOFA_D / 2 + 0.075);
  sofaBack.castShadow = true;
  house.add(sofaBack);

  // Sofa armrests
  for (const side of [-1, 1]) {
    const armrest = new THREE.Mesh(
      new THREE.BoxGeometry(0.12, 0.25, SOFA_D),
      sofaMat
    );
    armrest.position.set(lrCX + side * (SOFA_W / 2 - 0.06), SOFA_H + 0.125, SOFA_Z);
    armrest.castShadow = true;
    house.add(armrest);
  }

  // Seat cushions (3 cushions)
  for (let i = -1; i <= 1; i++) {
    const cushion = new THREE.Mesh(
      new THREE.BoxGeometry(SOFA_W / 3 - 0.04, 0.08, SOFA_D - 0.2),
      sofaCushionMat
    );
    cushion.position.set(lrCX + i * (SOFA_W / 3), SOFA_H + 0.04, SOFA_Z + 0.05);
    house.add(cushion);
  }

  // ============= BEDROOM FURNITURE =============
  const BED_SIZE = 1.83;
  const BED_H = 0.45; // bed height
  const mattressH = 0.18;

  const bedFrameMat = new THREE.MeshStandardMaterial({ color: 0x5c3a1e, roughness: 0.8 });
  const mattressMat = new THREE.MeshStandardMaterial({ color: 0xf0ece4, roughness: 0.9 });
  const pillowMat = new THREE.MeshStandardMaterial({ color: 0xffffff, roughness: 0.95 });

  // Center of each bedroom
  const bed1CenterX = BED_LEN / 2;
  const bed1CenterZ = HOUSE_WIDTH - BED_SIZE / 2 - 0.15; // near back wall with gap
  const bed2CenterX = LR_END + BED_LEN / 2;
  const bed2CenterZ = HOUSE_WIDTH - BED_SIZE / 2 - 0.15; // near back wall with gap

  for (const [cx, cz] of [[bed1CenterX, bed1CenterZ], [bed2CenterX, bed2CenterZ]]) {
    // Bed frame
    const frame = new THREE.Mesh(
      new THREE.BoxGeometry(BED_SIZE, BED_H - mattressH, BED_SIZE),
      bedFrameMat
    );
    frame.position.set(cx, (BED_H - mattressH) / 2, cz);
    frame.castShadow = true;
    frame.receiveShadow = true;
    house.add(frame);

    // Mattress
    const mattress = new THREE.Mesh(
      new THREE.BoxGeometry(BED_SIZE - 0.04, mattressH, BED_SIZE - 0.04),
      mattressMat
    );
    mattress.position.set(cx, BED_H - mattressH / 2, cz);
    mattress.castShadow = true;
    mattress.receiveShadow = true;
    house.add(mattress);

    // Headboard (against the back wall side)
    const headboard = new THREE.Mesh(
      new THREE.BoxGeometry(BED_SIZE, 0.5, 0.06),
      bedFrameMat
    );
    headboard.position.set(cx, BED_H + 0.25, cz + BED_SIZE / 2);
    headboard.castShadow = true;
    house.add(headboard);

    // Two pillows
    for (const pz of [cz + BED_SIZE / 2 - 0.25]) {
      for (const px of [cx - 0.3, cx + 0.3]) {
        const pillow = new THREE.Mesh(
          new THREE.BoxGeometry(0.4, 0.08, 0.25),
          pillowMat
        );
        pillow.position.set(px, BED_H + 0.04, pz);
        pillow.castShadow = true;
        house.add(pillow);
      }
    }
  }

  // ============= VERANDA DECK =============
  const veranda = new THREE.Group();
  const DECK_LENGTH = HOUSE_LENGTH; // spans full front
  const DECK_DEPTH = 3.2; // extends 3.2m from front wall
  const DECK_HEIGHT = 0.0; // flush with interior floor

  // Tiled deck surface (solid slab with tile material on top)
  const tileSlab = new THREE.Mesh(
    new THREE.BoxGeometry(DECK_LENGTH, 0.1, DECK_DEPTH),
    mat.deckTile
  );
  tileSlab.position.set(DECK_LENGTH / 2, -0.05, -DECK_DEPTH / 2);
  tileSlab.receiveShadow = true;
  tileSlab.castShadow = true;
  veranda.add(tileSlab);

  // Deck subframe (support beams underneath)
  const beamCount = 4;
  for (let i = 0; i < beamCount; i++) {
    const x = (DECK_LENGTH / (beamCount + 1)) * (i + 1);
    const beam = new THREE.Mesh(
      new THREE.BoxGeometry(0.1, 0.12, DECK_DEPTH),
      mat.concrete
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
